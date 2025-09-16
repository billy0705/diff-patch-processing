#define DUCKDB_EXTENSION_MAIN

#include "diff_patch_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
// For bind-time constant extraction
#include "duckdb/planner/expression/bound_constant_expression.hpp"
// Vector executors
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

// JSON parser (yyjson comes with DuckDB)
#include "yyjson.hpp"
#include "include/diff_lib.hpp"
// DuckDB vendors yyjson under namespace duckdb_yyjson
using namespace duckdb_yyjson;

// OpenSSL linked through vcpkg
// #include <openssl/opensslv.h>

namespace duckdb {

// UTF-8 helpers: advance by code points and count them safely
static inline size_t Utf8CharLen(unsigned char lead) {
	if (lead < 0x80)
		return 1; // 0xxxxxxx
	if ((lead >> 5) == 0x6)
		return 2; // 110xxxxx
	if ((lead >> 4) == 0xE)
		return 3; // 1110xxxx
	if ((lead >> 3) == 0x1E)
		return 4; // 11110xxx
	return 1;     // invalid lead, treat as single byte to avoid looping
}

static inline bool IsUtf8Continuation(unsigned char c) {
	return (c & 0xC0) == 0x80;
}

static inline size_t Utf8AdvanceBytes(const std::string &s, size_t start_byte, size_t n_codepoints) {
	size_t i = start_byte;
	size_t end = s.size();
	size_t advanced = 0;
	while (i < end && n_codepoints > 0) {
		unsigned char lead = static_cast<unsigned char>(s[i]);
		size_t clen = Utf8CharLen(lead);
		if (i + clen > end) {
			// truncated sequence; advance 1 to avoid infinite loop
			clen = 1;
		} else if (clen > 1) {
			// validate continuation bytes; if invalid, fall back to 1-byte advance
			for (size_t k = 1; k < clen; ++k) {
				if (!IsUtf8Continuation(static_cast<unsigned char>(s[i + k]))) {
					clen = 1;
					break;
				}
			}
		}
		i += clen;
		advanced += clen;
		--n_codepoints;
	}
	return advanced; // number of bytes consumed for n_codepoints (or until end)
}

static inline size_t Utf8CountCodepoints(const char *data, size_t len) {
	size_t i = 0;
	size_t count = 0;
	while (i < len) {
		unsigned char lead = static_cast<unsigned char>(data[i]);
		size_t clen = Utf8CharLen(lead);
		if (i + clen > len)
			clen = 1; // truncated tail
		else if (clen > 1) {
			for (size_t k = 1; k < clen; ++k) {
				if (!IsUtf8Continuation(static_cast<unsigned char>(data[i + k]))) {
					clen = 1;
					break;
				}
			}
		}
		i += clen;
		++count;
	}
	return count;
}

inline void DiffPatchScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// Implements patch_decompress2(old, js) where js is a JSON
	// array like: [["=", 5], ["+", "foo"], ["-", 2], ...]
	auto &old_col = args.data[0];
	auto &patch_col = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    old_col, patch_col, result, args.size(), [&](string_t old_str_t, string_t patch_str_t) -> string_t {
		    // Convert input vectors to std::string
		    std::string old_str = old_str_t.GetString();
		    std::string patch_str = patch_str_t.GetString();

		    // Parse JSON using yyjson
		    yyjson_doc *doc = yyjson_read(patch_str.c_str(), patch_str.size(), 0);
		    if (!doc) {
			    // If parsing fails, return old unchanged (or empty). Here: return old
			    return StringVector::AddString(result, old_str);
		    }
		    yyjson_val *root = yyjson_doc_get_root(doc);
		    if (!root || !yyjson_is_arr(root)) {
			    yyjson_doc_free(doc);
			    return StringVector::AddString(result, old_str);
		    }

		    size_t idx = 0;
		    std::string out;
		    out.reserve(old_str.size() + 64);

		    yyjson_arr_iter it;
		    yyjson_arr_iter_init(root, &it);
		    yyjson_val *elem;
		    while ((elem = yyjson_arr_iter_next(&it))) {
			    if (!yyjson_is_arr(elem) || yyjson_arr_size(elem) < 2) {
				    continue; // ignore malformed ops
			    }
			    yyjson_val *tag_val = yyjson_arr_get(elem, 0);
			    yyjson_val *val_val = yyjson_arr_get(elem, 1);
			    const char *tag = yyjson_get_str(tag_val);
			    if (!tag) {
				    continue;
			    }

			    if (tag[0] == '=' && tag[1] == '\0') {
				    long long n = 0;
				    if (yyjson_is_int(val_val)) {
					    n = yyjson_get_int(val_val);
				    } else if (yyjson_is_str(val_val)) {
					    const char *s = yyjson_get_str(val_val);
					    n = s ? atoll(s) : 0;
				    }
				    if (n < 0)
					    n = 0;
				    // advance by n UTF-8 code points from current byte index
				    if (idx > old_str.size())
					    idx = old_str.size();
				    size_t remain_bytes = old_str.size() - idx;
				    size_t bytes_to_take = Utf8AdvanceBytes(old_str, idx, (size_t)n);
				    if (bytes_to_take > remain_bytes)
					    bytes_to_take = remain_bytes;
				    out.append(old_str.data() + idx, bytes_to_take);
				    idx += bytes_to_take;
			    } else if (tag[0] == '+' && tag[1] == '\0') {
				    if (yyjson_is_str(val_val)) {
					    const char *s = yyjson_get_str(val_val);
					    if (s)
						    out.append(s);
				    } else if (yyjson_is_int(val_val)) {
					    char buf[32];
					    int len = snprintf(buf, sizeof(buf), "%lld", (long long)yyjson_get_int(val_val));
					    if (len > 0)
						    out.append(buf, (size_t)len);
				    } else if (yyjson_is_real(val_val)) {
					    char buf[64];
					    int len = snprintf(buf, sizeof(buf), "%g", yyjson_get_real(val_val));
					    if (len > 0)
						    out.append(buf, (size_t)len);
				    }
			    } else if (tag[0] == '-' && tag[1] == '\0') {
				    long long n = 0;
				    if (yyjson_is_int(val_val)) {
					    n = yyjson_get_int(val_val);
				    } else if (yyjson_is_str(val_val)) {
					    const char *s = yyjson_get_str(val_val);
					    n = s ? atoll(s) : 0;
				    }
				    if (n < 0)
					    n = 0;
				    if (idx > old_str.size())
					    idx = old_str.size();
				    size_t remain_bytes = old_str.size() - idx;
				    size_t bytes_to_skip = Utf8AdvanceBytes(old_str, idx, (size_t)n);
				    if (bytes_to_skip > remain_bytes)
					    bytes_to_skip = remain_bytes;
				    idx += bytes_to_skip;
			    } else {
				    // ignore unknown op
			    }
		    }

		    yyjson_doc_free(doc);
		    return StringVector::AddString(result, out);
	    });
}

inline void PatchLenScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// Computes the resulting UTF-8 code point length applying a patch JSON
	// Only '=' (copy n) and '+' (insert s) contribute to the final length
	auto &patch_col = args.data[0];
	UnaryExecutor::Execute<string_t, int64_t>(patch_col, result, args.size(), [&](string_t patch_str_t) -> int64_t {
		std::string patch_str = patch_str_t.GetString();
		yyjson_doc *doc = yyjson_read(patch_str.c_str(), patch_str.size(), 0);
		if (!doc) {
			return 0;
		}
		yyjson_val *root = yyjson_doc_get_root(doc);
		if (!root || !yyjson_is_arr(root)) {
			yyjson_doc_free(doc);
			return 0;
		}
		long long total = 0;
		yyjson_arr_iter it;
		yyjson_arr_iter_init(root, &it);
		yyjson_val *elem;
		while ((elem = yyjson_arr_iter_next(&it))) {
			if (!yyjson_is_arr(elem) || yyjson_arr_size(elem) < 2) {
				continue;
			}
			yyjson_val *tag_val = yyjson_arr_get(elem, 0);
			yyjson_val *val_val = yyjson_arr_get(elem, 1);
			const char *tag = yyjson_get_str(tag_val);
			if (!tag) {
				continue;
			}
			if (tag[0] == '+' && tag[1] == '\0') {
				if (yyjson_is_str(val_val)) {
					// Count inserted string in code points, not bytes
					const char *s = yyjson_get_str(val_val);
					size_t blen = (size_t)yyjson_get_len(val_val);
					total += (long long)Utf8CountCodepoints(s ? s : "", blen);
				} else if (yyjson_is_int(val_val)) {
					char buf[32];
					int len = snprintf(buf, sizeof(buf), "%lld", (long long)yyjson_get_int(val_val));
					if (len > 0)
						total += len;
				} else if (yyjson_is_real(val_val)) {
					char buf[64];
					int len = snprintf(buf, sizeof(buf), "%g", yyjson_get_real(val_val));
					if (len > 0)
						total += len;
				}
			} else if (tag[0] == '=' && tag[1] == '\0') {
				long long n = 0;
				if (yyjson_is_int(val_val)) {
					n = yyjson_get_int(val_val);
				} else if (yyjson_is_str(val_val)) {
					const char *s = yyjson_get_str(val_val);
					n = s ? atoll(s) : 0;
				}
				if (n > 0)
					total += n;
			} else {
				// '-' and unknown ops do not contribute
			}
		}
		yyjson_doc_free(doc);
		return (int64_t)total;
	});
}

// Reconstruct new content from (old_content, ops string, plus_concat string, vals list)
inline void ApplyColsScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// args: [0]=old_content VARCHAR, [1]=ops VARCHAR, [2]=plus_concat VARCHAR, [3]=vals LIST<BIGINT>
	auto &old_col = args.data[0];
	auto &ops_col = args.data[1];
	auto &plus_col = args.data[2];
	auto &vals_col = args.data[3];

	old_col.Flatten(args.size());
	ops_col.Flatten(args.size());
	plus_col.Flatten(args.size());
	vals_col.Flatten(args.size());

	auto old_data = FlatVector::GetData<string_t>(old_col);
	auto ops_data = FlatVector::GetData<string_t>(ops_col);
	auto plus_data = FlatVector::GetData<string_t>(plus_col);
	auto list_entries = FlatVector::GetData<list_entry_t>(vals_col);
	auto &vals_child = ListVector::GetEntry(vals_col);
	auto vals_child_data = FlatVector::GetData<int64_t>(vals_child);

	auto out_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		if (FlatVector::IsNull(old_col, i) || FlatVector::IsNull(ops_col, i) || FlatVector::IsNull(plus_col, i) ||
		    FlatVector::IsNull(vals_col, i)) {
			// if any input is NULL, return NULL
			FlatVector::SetNull(result, i, true);
			continue;
		}

		std::string old_s = old_data[i].GetString();
		std::string ops_s = ops_data[i].GetString();
		std::string plus_s = plus_data[i].GetString();

		// gather vals for this row
		std::vector<int64_t> vals;
		auto entry = list_entries[i];
		vals.reserve(entry.length);
		for (idx_t j = 0; j < entry.length; j++) {
			vals.push_back(vals_child_data[entry.offset + j]);
		}

		size_t old_idx = 0;       // byte index into old_s
		size_t ins_cp_idx = 0;    // codepoint index processed from plus_s
		size_t plus_byte_idx = 0; // byte index into plus_s corresponding to ins_cp_idx
		std::string out;
		out.reserve(old_s.size() + plus_s.size());

		// iterate operations; vals should have same length
		size_t nops = ops_s.size();
		size_t nvals = vals.size();
		size_t steps = std::min(nops, nvals);
		for (size_t k = 0; k < steps; k++) {
			char op = ops_s[k];
			int64_t v = vals[k];
			if (op == '=') {
				if (v < 0)
					v = 0;
				size_t bytes = Utf8AdvanceBytes(old_s, old_idx, (size_t)v);
				if (bytes > 0) {
					out.append(old_s.data() + old_idx, bytes);
					old_idx += bytes;
				}
			} else if (op == '-') {
				if (v < 0)
					v = 0;
				size_t bytes = Utf8AdvanceBytes(old_s, old_idx, (size_t)v);
				old_idx += bytes;
			} else if (op == '+') {
				// v is the cumulative codepoint end position in plus_s after this insertion
				if (v < 0)
					v = 0;
				size_t end_cp = (size_t)v;
				if (end_cp > ins_cp_idx) {
					size_t seg_cp = end_cp - ins_cp_idx;
					size_t bytes = Utf8AdvanceBytes(plus_s, plus_byte_idx, seg_cp);
					if (bytes > 0) {
						out.append(plus_s.data() + plus_byte_idx, bytes);
						plus_byte_idx += bytes;
						ins_cp_idx = end_cp;
					}
				}
			} else {
				// ignore unknown op
			}
		}

		out_data[i] = StringVector::AddString(result, out);
	}
}

// Compute resulting codepoint length from ops/vals only
inline void ColsLenScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &ops_col = args.data[0];
	auto &vals_col = args.data[1];
	ops_col.Flatten(args.size());
	vals_col.Flatten(args.size());

	auto ops_data = FlatVector::GetData<string_t>(ops_col);
	auto list_entries = FlatVector::GetData<list_entry_t>(vals_col);
	auto &vals_child = ListVector::GetEntry(vals_col);
	auto vals_child_data = FlatVector::GetData<int64_t>(vals_child);
	auto out_data = FlatVector::GetData<int64_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		if (FlatVector::IsNull(ops_col, i) || FlatVector::IsNull(vals_col, i)) {
			out_data[i] = 0;
			continue;
		}
		auto ops_s = ops_data[i].GetString();
		auto entry = list_entries[i];
		idx_t steps = MinValue<idx_t>(ops_s.size(), entry.length);
		long long total_eq = 0;
		long long last_plus_cp = 0;
		for (idx_t k = 0; k < steps; k++) {
			char op = ops_s[k];
			long long v = vals_child_data[entry.offset + k];
			if (v < 0)
				v = 0;
			if (op == '=') {
				total_eq += v;
			} else if (op == '+') {
				last_plus_cp = v;
			} else {
				// '-': no contribution
			}
		}
		out_data[i] = total_eq + last_plus_cp;
	}
}

// Build three derived columns from a patch JSON:
// 1) ops: a string with the sequence of operation tags (e.g., "=+-+")
// 2) plus_concat: concatenation of all '+' insertion payloads converted to string
// 3) vals: LIST<BIGINT> with numbers: for '=' and '-' the numeric argument, for '+'
//    the cumulative UTF-8 codepoint length of plus_concat after this insertion
static void MakeColsFromPatchJSON(const std::string &patch_json, std::string &ops_out, std::string &plus_concat_out,
                                  std::vector<int64_t> &vals_out) {
	ops_out.clear();
	plus_concat_out.clear();
	vals_out.clear();

	idx_t ins_codepoints_total = 0;

	// Parse JSON
	yyjson_doc *doc = yyjson_read(patch_json.c_str(), patch_json.size(), 0);
	if (!doc) {
		return; // leave outputs empty
	}
	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_arr(root)) {
		yyjson_doc_free(doc);
		return;
	}

	yyjson_arr_iter it;
	yyjson_arr_iter_init(root, &it);
	yyjson_val *elem;
	while ((elem = yyjson_arr_iter_next(&it))) {
		if (!yyjson_is_arr(elem) || yyjson_arr_size(elem) < 2) {
			continue;
		}
		yyjson_val *tag_val = yyjson_arr_get(elem, 0);
		yyjson_val *val_val = yyjson_arr_get(elem, 1);
		const char *tag = yyjson_get_str(tag_val);
		if (!tag || tag[1] != '\0') {
			continue;
		}
		char t = tag[0];
		if (t == '=') {
			long long n = 0;
			if (yyjson_is_int(val_val)) {
				n = yyjson_get_int(val_val);
			} else if (yyjson_is_str(val_val)) {
				const char *s = yyjson_get_str(val_val);
				n = s ? atoll(s) : 0;
			}
			if (n < 0)
				n = 0;
			ops_out.push_back('=');
			vals_out.push_back((int64_t)n);
		} else if (t == '+') {
			// Convert payload to string and append to plus_concat_out
			std::string ins;
			if (yyjson_is_str(val_val)) {
				const char *s = yyjson_get_str(val_val);
				if (s)
					ins.assign(s, yyjson_get_len(val_val));
			} else if (yyjson_is_int(val_val)) {
				char buf[32];
				int len = snprintf(buf, sizeof(buf), "%lld", (long long)yyjson_get_int(val_val));
				if (len > 0)
					ins.assign(buf, (size_t)len);
			} else if (yyjson_is_real(val_val)) {
				char buf[64];
				int len = snprintf(buf, sizeof(buf), "%g", yyjson_get_real(val_val));
				if (len > 0)
					ins.assign(buf, (size_t)len);
			}
			if (!ins.empty()) {
				plus_concat_out.append(ins);
				// Count codepoints in the inserted substring
				ins_codepoints_total += (idx_t)Utf8CountCodepoints(ins.c_str(), ins.size());
			}
			ops_out.push_back('+');
			vals_out.push_back((int64_t)ins_codepoints_total);
		} else if (t == '-') {
			long long n = 0;
			if (yyjson_is_int(val_val)) {
				n = yyjson_get_int(val_val);
			} else if (yyjson_is_str(val_val)) {
				const char *s = yyjson_get_str(val_val);
				n = s ? atoll(s) : 0;
			}
			if (n < 0)
				n = 0;
			ops_out.push_back('-');
			vals_out.push_back((int64_t)n);
		} else {
			// ignore unknown op
		}
	}
	yyjson_doc_free(doc);
}

// ---- Dynamic-named outputs ----
// Bind for make_cols(patch_json, new_col_name)
static unique_ptr<FunctionData> MakeColsBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("make_cols expects (patch_json, new_col_name)");
	}
	string base = "col";
	if (arguments[1]->expression_class == ExpressionClass::BOUND_CONSTANT) {
		auto &cexpr = arguments[1]->Cast<BoundConstantExpression>();
		if (!cexpr.value.IsNull()) {
			base = cexpr.value.GetValue<string>();
		}
	} else {
		throw BinderException("make_cols: new_col_name must be a constant string");
	}
	child_list_t<LogicalType> struct_children;
	struct_children.emplace_back(base + "_ops", LogicalType::VARCHAR);
	struct_children.emplace_back(base + "_plus_concat", LogicalType::VARCHAR);
	struct_children.emplace_back(base + "_vals", LogicalType::LIST(LogicalType::BIGINT));
	bound_function.return_type = LogicalType::STRUCT(std::move(struct_children));
	return nullptr;
}

inline void MakeColsNamedFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// Inputs: (patch_json VARCHAR, new_col_name VARCHAR)
	idx_t count = args.size();
	auto &patch_col = args.data[0];
	patch_col.Flatten(count);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &children = StructVector::GetEntries(result);
	auto &ops_vec = *children[0];  // VARCHAR
	auto &plus_vec = *children[1]; // VARCHAR
	auto &vals_vec = *children[2]; // LIST<BIGINT>

	auto ops_data = FlatVector::GetData<string_t>(ops_vec);
	auto plus_data = FlatVector::GetData<string_t>(plus_vec);
	auto list_entries = FlatVector::GetData<list_entry_t>(vals_vec);
	auto &vals_child = ListVector::GetEntry(vals_vec);
	auto vals_child_data = FlatVector::GetData<int64_t>(vals_child);

	idx_t current_list_size = ListVector::GetListSize(vals_vec);

	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(patch_col, i)) {
			ops_data[i] = StringVector::AddString(ops_vec, "");
			plus_data[i] = StringVector::AddString(plus_vec, "");
			list_entries[i].offset = current_list_size;
			list_entries[i].length = 0;
			continue;
		}
		auto patch_str_t = FlatVector::GetData<string_t>(patch_col)[i];
		std::string patch_json = patch_str_t.GetString();

		std::string ops, plus_concat;
		std::vector<int64_t> vals;
		MakeColsFromPatchJSON(patch_json, ops, plus_concat, vals);

		ops_data[i] = StringVector::AddString(ops_vec, ops);
		plus_data[i] = StringVector::AddString(plus_vec, plus_concat);

		idx_t needed = vals.size();
		if (needed > 0) {
			ListVector::Reserve(vals_vec, current_list_size + needed);
			// reacquire pointer in case of reallocation
			vals_child_data = FlatVector::GetData<int64_t>(vals_child);
			for (idx_t j = 0; j < needed; j++) {
				vals_child_data[current_list_size + j] = vals[j];
			}
		}
		list_entries[i].offset = current_list_size;
		list_entries[i].length = needed;
		current_list_size += needed;
	}
	ListVector::SetListSize(vals_vec, current_list_size);
}

// Bind for make_cols_from_text(old_content, new_content, new_col_name)
static unique_ptr<FunctionData> MakeColsFromTextBind(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 3) {
		throw BinderException("make_cols_from_text expects (old_content, new_content, new_col_name)");
	}
	string base = "col";
	if (arguments[2]->expression_class == ExpressionClass::BOUND_CONSTANT) {
		auto &cexpr = arguments[2]->Cast<BoundConstantExpression>();
		if (!cexpr.value.IsNull()) {
			base = cexpr.value.GetValue<string>();
		}
	} else {
		throw BinderException("make_cols_from_text: new_col_name must be a constant string");
	}
	child_list_t<LogicalType> struct_children;
	struct_children.emplace_back(base + "_ops", LogicalType::VARCHAR);
	struct_children.emplace_back(base + "_plus_concat", LogicalType::VARCHAR);
	struct_children.emplace_back(base + "_vals", LogicalType::LIST(LogicalType::BIGINT));
	bound_function.return_type = LogicalType::STRUCT(std::move(struct_children));
	return nullptr;
}

inline void MakeColsFromTextFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// Inputs: (old_content VARCHAR, new_content VARCHAR, new_col_name VARCHAR)
	idx_t count = args.size();
	auto &old_col = args.data[0];
	auto &new_col = args.data[1];
	old_col.Flatten(count);
	new_col.Flatten(count);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &children = StructVector::GetEntries(result);
	auto &ops_vec = *children[0];
	auto &plus_vec = *children[1];
	auto &vals_vec = *children[2];

	auto ops_data = FlatVector::GetData<string_t>(ops_vec);
	auto plus_data = FlatVector::GetData<string_t>(plus_vec);
	auto list_entries = FlatVector::GetData<list_entry_t>(vals_vec);
	auto &vals_child = ListVector::GetEntry(vals_vec);
	auto vals_child_data = FlatVector::GetData<int64_t>(vals_child);

	idx_t current_list_size = ListVector::GetListSize(vals_vec);

	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(old_col, i) || FlatVector::IsNull(new_col, i)) {
			ops_data[i] = StringVector::AddString(ops_vec, "");
			plus_data[i] = StringVector::AddString(plus_vec, "");
			list_entries[i].offset = current_list_size;
			list_entries[i].length = 0;
			continue;
		}
		auto old_t = FlatVector::GetData<string_t>(old_col)[i];
		auto new_t = FlatVector::GetData<string_t>(new_col)[i];
		std::string old_s = old_t.GetString();
		std::string new_s = new_t.GetString();
		std::string patch_json = diffpatch::GeneratePatchJson(old_s, new_s, false, nullptr);

		std::string ops, plus_concat;
		std::vector<int64_t> vals;
		MakeColsFromPatchJSON(patch_json, ops, plus_concat, vals);

		ops_data[i] = StringVector::AddString(ops_vec, ops);
		plus_data[i] = StringVector::AddString(plus_vec, plus_concat);

		idx_t needed = vals.size();
		if (needed > 0) {
			ListVector::Reserve(vals_vec, current_list_size + needed);
			vals_child_data = FlatVector::GetData<int64_t>(vals_child);
			for (idx_t j = 0; j < needed; j++) {
				vals_child_data[current_list_size + j] = vals[j];
			}
		}
		list_entries[i].offset = current_list_size;
		list_entries[i].length = needed;
		current_list_size += needed;
	}
	ListVector::SetListSize(vals_vec, current_list_size);
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto diff_patch_scalar_function = ScalarFunction("diff_patch", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                                 LogicalType::VARCHAR, DiffPatchScalarFun);
	ExtensionUtil::RegisterFunction(instance, diff_patch_scalar_function);

	// Register patch_len(patch_json) -> BIGINT
	auto patch_len_scalar_function =
	    ScalarFunction("patch_len", {LogicalType::VARCHAR}, LogicalType::BIGINT, PatchLenScalarFun);
	ExtensionUtil::RegisterFunction(instance, patch_len_scalar_function);

	// Register make_patch(old, new) -> VARCHAR(JSON)
	auto make_patch_scalar_function =
	    ScalarFunction("make_patch", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                   [](DataChunk &args, ExpressionState &state, Vector &result) {
		                   auto &old_col = args.data[0];
		                   auto &new_col = args.data[1];
		                   BinaryExecutor::Execute<string_t, string_t, string_t>(
		                       old_col, new_col, result, args.size(), [&](string_t old_t, string_t new_t) -> string_t {
			                       std::string old_s = old_t.GetString();
			                       std::string new_s = new_t.GetString();
			                       std::string js = diffpatch::GeneratePatchJson(old_s, new_s, false, nullptr);
			                       return StringVector::AddString(result, js);
		                       });
	                   });
	ExtensionUtil::RegisterFunction(instance, make_patch_scalar_function);

	// Register make_cols(patch_json, new_col_name) with dynamic field names
	{
		child_list_t<LogicalType> dummy_children;
		dummy_children.emplace_back("ops", LogicalType::VARCHAR);
		dummy_children.emplace_back("plus_concat", LogicalType::VARCHAR);
		dummy_children.emplace_back("vals", LogicalType::LIST(LogicalType::BIGINT));
		ScalarFunction make_cols_fn("make_cols", {LogicalType::VARCHAR, LogicalType::VARCHAR},
		                            LogicalType::STRUCT(std::move(dummy_children)), MakeColsNamedFun, MakeColsBind);
		ExtensionUtil::RegisterFunction(instance, make_cols_fn);
	}

	// Register make_cols_from_text(old_content, new_content, new_col_name)
	{
		child_list_t<LogicalType> dummy_children;
		dummy_children.emplace_back("ops", LogicalType::VARCHAR);
		dummy_children.emplace_back("plus_concat", LogicalType::VARCHAR);
		dummy_children.emplace_back("vals", LogicalType::LIST(LogicalType::BIGINT));
		ScalarFunction make_cols_text_fn(
		    "make_cols_from_text", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
		    LogicalType::STRUCT(std::move(dummy_children)), MakeColsFromTextFun, MakeColsFromTextBind);
		ExtensionUtil::RegisterFunction(instance, make_cols_text_fn);
	}

	// Register apply_cols(old_content, ops, plus_concat, vals) -> VARCHAR
	{
		ScalarFunction apply_cols_fn(
		    "apply_cols",
		    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::BIGINT)},
		    LogicalType::VARCHAR, ApplyColsScalarFun);
		ExtensionUtil::RegisterFunction(instance, apply_cols_fn);
	}

	// Register cols_len(ops, vals) -> BIGINT
	{
		ScalarFunction cols_len_fn("cols_len", {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::BIGINT)},
		                           LogicalType::BIGINT, ColsLenScalarFun);
		ExtensionUtil::RegisterFunction(instance, cols_len_fn);
	}
}

void DiffPatchExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string DiffPatchExtension::Name() {
	return "diff_patch";
}

std::string DiffPatchExtension::Version() const {
#ifdef EXT_VERSION_DIFF_PATCH
	return EXT_VERSION_DIFF_PATCH;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void diff_patch_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::DiffPatchExtension>();
}

DUCKDB_EXTENSION_API const char *diff_patch_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
