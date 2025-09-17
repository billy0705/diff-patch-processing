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
#include "include/utf8_utils.hpp"
#include "include/patch_json_utils.hpp"
#include "include/diff_lib.hpp"
// DuckDB vendors yyjson under namespace duckdb_yyjson
using namespace duckdb_yyjson;

// OpenSSL linked through vcpkg
// #include <openssl/opensslv.h>

namespace duckdb {

// UTF-8 helpers are now provided by include/utf8_utils.hpp

inline void DiffPatchScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// Implements patch_decompress2(old, js) where js is a JSON
	// array like: [["=", 5], ["+", "foo"], ["-", 2], ...]
	auto &old_col = args.data[0];
	auto &patch_col = args.data[1];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		bool old_is_null = FlatVector::IsNull(old_col, i);
		bool patch_is_null = FlatVector::IsNull(patch_col, i);

		if (patch_is_null) {
			if (old_is_null) {
				FlatVector::SetNull(result, i, true);
			} else {
				auto old_t = FlatVector::GetData<string_t>(old_col)[i];
				out_data[i] = StringVector::AddString(result, old_t.GetString());
			}
			continue;
		}

		std::string old_str = old_is_null ? std::string() : FlatVector::GetData<string_t>(old_col)[i].GetString();
		std::string patch_str = FlatVector::GetData<string_t>(patch_col)[i].GetString();

		std::vector<patchjson::PatchOp> ops;
		if (!patchjson::ParsePatchJSON(patch_str, ops)) {
			out_data[i] = StringVector::AddString(result, old_str);
			continue;
		}
		if (ops.empty()) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Build codepoint->byte offsets for old_str to avoid per-step scans
		std::vector<uint32_t> dummy_cp;
		std::vector<size_t> old_offs;
		Utf8ToCodepoints(old_str, dummy_cp, old_offs);
		idx_t old_cplen = old_offs.empty() ? 0 : (idx_t)old_offs.size() - 1;
		idx_t old_cp = 0;

		std::string out;
		out.reserve(old_str.size() + 64);
		for (auto &op : ops) {
			if (op.tag == '=') {
				idx_t take = (idx_t)op.count;
				if (take < 0)
					take = 0;
				if (old_cp + take > old_cplen)
					take = old_cplen - old_cp;
				if (take > 0) {
					size_t b0 = old_offs[old_cp];
					size_t b1 = old_offs[old_cp + take];
					out.append(old_str.data() + b0, b1 - b0);
					old_cp += take;
				}
			} else if (op.tag == '+') {
				out.append(op.insert);
			} else if (op.tag == '-') {
				idx_t skip = (idx_t)op.count;
				if (skip < 0)
					skip = 0;
				if (old_cp + skip > old_cplen)
					skip = old_cplen - old_cp;
				old_cp += skip;
			}
		}
		out_data[i] = StringVector::AddString(result, out);
	}
}

inline void PatchLenScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	// Computes the resulting UTF-8 code point length applying a patch JSON
	// Only '=' (copy n) and '+' (insert s) contribute to the final length
	auto &patch_col = args.data[0];
	UnaryExecutor::Execute<string_t, int64_t>(patch_col, result, args.size(), [&](string_t patch_str_t) -> int64_t {
		std::string patch_str = patch_str_t.GetString();
		std::vector<patchjson::PatchOp> ops;
		if (!patchjson::ParsePatchJSON(patch_str, ops)) {
			return 0;
		}
		long long total = 0;
		for (auto &op : ops) {
			if (op.tag == '+') {
				total += (long long)Utf8CountCodepoints(op.insert.c_str(), op.insert.size());
			} else if (op.tag == '=') {
				if (op.count > 0)
					total += (long long)op.count;
			}
		}
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

	// Ensure we are writing into a flat vector
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		bool old_is_null = FlatVector::IsNull(old_col, i);
		bool ops_is_null = FlatVector::IsNull(ops_col, i);
		bool plus_is_null = FlatVector::IsNull(plus_col, i);
		bool vals_is_null = FlatVector::IsNull(vals_col, i);

		// If any of ops/plus/vals is NULL → result is NULL
		if (ops_is_null || plus_is_null || vals_is_null) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		std::string ops_s = ops_data[i].GetString();
		std::string plus_s = plus_data[i].GetString();

		std::vector<int64_t> vals;
		{
			auto entry = list_entries[i];
			vals.reserve(entry.length);
			for (idx_t j = 0; j < entry.length; j++) {
				vals.push_back(vals_child_data[entry.offset + j]);
			}
		}

		// If ops/plus/vals are all empty, return NULL (represents no-op)
		if (ops_s.empty() && plus_s.empty() && vals.empty()) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		std::string old_s = old_is_null ? std::string() : old_data[i].GetString();

		// Build codepoint->byte offsets for fast slicing
		std::vector<uint32_t> d1, d2;
		std::vector<size_t> old_offs2, plus_offs;
		Utf8ToCodepoints(old_s, d1, old_offs2);
		Utf8ToCodepoints(plus_s, d2, plus_offs);
		idx_t old_cplen2 = old_offs2.empty() ? 0 : (idx_t)old_offs2.size() - 1;
		idx_t plus_cplen = plus_offs.empty() ? 0 : (idx_t)plus_offs.size() - 1;
		idx_t old_cp2 = 0;
		idx_t ins_cp_idx2 = 0; // cumulative cp consumed from plus_s

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
				idx_t take = (idx_t)v;
				if (old_cp2 + take > old_cplen2)
					take = old_cplen2 - old_cp2;
				if (take > 0) {
					size_t b0 = old_offs2[old_cp2];
					size_t b1 = old_offs2[old_cp2 + take];
					out.append(old_s.data() + b0, b1 - b0);
					old_cp2 += take;
				}
			} else if (op == '-') {
				if (v < 0)
					v = 0;
				idx_t skip = (idx_t)v;
				if (old_cp2 + skip > old_cplen2)
					skip = old_cplen2 - old_cp2;
				old_cp2 += skip;
			} else if (op == '+') {
				// v is the cumulative codepoint end position in plus_s after this insertion
				if (v < 0)
					v = 0;
				size_t end_cp = (size_t)v;
				if (end_cp > (size_t)plus_cplen)
					end_cp = plus_cplen;
				if (end_cp > (size_t)ins_cp_idx2) {
					size_t b0 = plus_offs[ins_cp_idx2];
					size_t b1 = plus_offs[end_cp];
					out.append(plus_s.data() + b0, b1 - b0);
					ins_cp_idx2 = (idx_t)end_cp;
				}
			} else {
				// ignore unknown op
			}
		}

		// Return empty string if result is empty, not NULL
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
	std::vector<patchjson::PatchOp> ops;
	if (!patchjson::ParsePatchJSON(patch_json, ops)) {
		return;
	}
	for (auto &op : ops) {
		if (op.tag == '=') {
			ops_out.push_back('=');
			vals_out.push_back((int64_t)std::max<int64_t>(0, op.count));
		} else if (op.tag == '+') {
			if (!op.insert.empty()) {
				plus_concat_out.append(op.insert);
				ins_codepoints_total += (idx_t)Utf8CountCodepoints(op.insert.c_str(), op.insert.size());
			}
			ops_out.push_back('+');
			vals_out.push_back((int64_t)ins_codepoints_total);
		} else if (op.tag == '-') {
			ops_out.push_back('-');
			vals_out.push_back((int64_t)std::max<int64_t>(0, op.count));
		} else {
			// ignore
		}
	}
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

	// Prepass: parse and collect per-row ops for single Reserve
	std::vector<std::vector<patchjson::PatchOp>> row_ops(count);
	std::vector<idx_t> row_val_len(count, 0);
	std::vector<size_t> row_plus_bytes(count, 0);
	idx_t total_vals = 0;
	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(patch_col, i))
			continue;
		auto patch_str_t = FlatVector::GetData<string_t>(patch_col)[i];
		std::string patch_json = patch_str_t.GetString();
		if (!patchjson::ParsePatchJSON(patch_json, row_ops[i]))
			continue;
		idx_t vals_len = 0;
		size_t plus_bytes = 0;
		for (auto &op : row_ops[i]) {
			if (op.tag == '+' || op.tag == '-' || op.tag == '=') {
				vals_len++;
			}
			if (op.tag == '+')
				plus_bytes += op.insert.size();
		}
		row_val_len[i] = vals_len;
		row_plus_bytes[i] = plus_bytes;
		total_vals += vals_len;
	}
	// Reserve once for LIST child
	if (total_vals > 0) {
		ListVector::Reserve(vals_vec, total_vals);
		ListVector::SetListSize(vals_vec, total_vals);
		// reacquire child pointer after reserve
		vals_child_data = FlatVector::GetData<int64_t>(vals_child);
	}
	idx_t running_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(patch_col, i) || row_ops[i].empty()) {
			ops_data[i] = StringVector::AddString(ops_vec, "");
			plus_data[i] = StringVector::AddString(plus_vec, "");
			list_entries[i].offset = running_offset;
			list_entries[i].length = 0;
			continue;
		}
		auto &opsv = row_ops[i];
		std::string ops;
		ops.reserve(opsv.size());
		std::string plus_concat;
		plus_concat.reserve(row_plus_bytes[i]);
		idx_t ins_cp_total = 0;
		// Fill vals into pre-reserved child
		idx_t needed = row_val_len[i];
		list_entries[i].offset = running_offset;
		list_entries[i].length = needed;
		idx_t w = 0;
		for (auto &op : opsv) {
			if (op.tag == '=') {
				ops.push_back('=');
				vals_child_data[running_offset + w++] = (int64_t)std::max<int64_t>(0, op.count);
			} else if (op.tag == '+') {
				ops.push_back('+');
				if (!op.insert.empty()) {
					plus_concat.append(op.insert);
					ins_cp_total += (idx_t)Utf8CountCodepoints(op.insert.c_str(), op.insert.size());
				}
				vals_child_data[running_offset + w++] = (int64_t)ins_cp_total;
			} else if (op.tag == '-') {
				ops.push_back('-');
				vals_child_data[running_offset + w++] = (int64_t)std::max<int64_t>(0, op.count);
			} else {
				// ignore
			}
		}
		running_offset += needed;
		ops_data[i] = StringVector::AddString(ops_vec, ops);
		plus_data[i] = StringVector::AddString(plus_vec, plus_concat);
	}
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

	// Prepass: build patch ops for rows where new is not NULL
	std::vector<std::vector<patchjson::PatchOp>> row_ops(count);
	std::vector<idx_t> row_val_len(count, 0);
	std::vector<size_t> row_plus_bytes(count, 0);
	idx_t total_vals = 0;
	for (idx_t i = 0; i < count; i++) {
		bool old_is_null = FlatVector::IsNull(old_col, i);
		bool new_is_null = FlatVector::IsNull(new_col, i);
		if (new_is_null)
			continue;
		std::string old_s = old_is_null ? std::string() : FlatVector::GetData<string_t>(old_col)[i].GetString();
		std::string new_s = FlatVector::GetData<string_t>(new_col)[i].GetString();
		std::string patch_json = diffpatch::GeneratePatchJson(old_s, new_s, false, nullptr);
		if (!patchjson::ParsePatchJSON(patch_json, row_ops[i]))
			continue;
		idx_t vals_len = 0;
		size_t plus_bytes = 0;
		for (auto &op : row_ops[i]) {
			if (op.tag == '+' || op.tag == '-' || op.tag == '=')
				vals_len++;
			if (op.tag == '+')
				plus_bytes += op.insert.size();
		}
		row_val_len[i] = vals_len;
		row_plus_bytes[i] = plus_bytes;
		total_vals += vals_len;
	}
	if (total_vals > 0) {
		ListVector::Reserve(vals_vec, total_vals);
		ListVector::SetListSize(vals_vec, total_vals);
		vals_child_data = FlatVector::GetData<int64_t>(vals_child);
	}
	idx_t running_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		bool new_is_null = FlatVector::IsNull(new_col, i);
		if (new_is_null || row_ops[i].empty()) {
			ops_data[i] = StringVector::AddString(ops_vec, "");
			plus_data[i] = StringVector::AddString(plus_vec, "");
			list_entries[i].offset = running_offset;
			list_entries[i].length = 0;
			continue;
		}
		auto &opsv = row_ops[i];
		std::string ops;
		ops.reserve(opsv.size());
		std::string plus_concat;
		plus_concat.reserve(row_plus_bytes[i]);
		idx_t ins_cp_total = 0;
		idx_t needed = row_val_len[i];
		list_entries[i].offset = running_offset;
		list_entries[i].length = needed;
		idx_t w = 0;
		for (auto &op : opsv) {
			if (op.tag == '=') {
				ops.push_back('=');
				vals_child_data[running_offset + w++] = (int64_t)std::max<int64_t>(0, op.count);
			} else if (op.tag == '+') {
				ops.push_back('+');
				if (!op.insert.empty()) {
					plus_concat.append(op.insert);
					ins_cp_total += (idx_t)Utf8CountCodepoints(op.insert.c_str(), op.insert.size());
				}
				vals_child_data[running_offset + w++] = (int64_t)ins_cp_total;
			} else if (op.tag == '-') {
				ops.push_back('-');
				vals_child_data[running_offset + w++] = (int64_t)std::max<int64_t>(0, op.count);
			}
		}
		running_offset += needed;
		ops_data[i] = StringVector::AddString(ops_vec, ops);
		plus_data[i] = StringVector::AddString(plus_vec, plus_concat);
	}
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto diff_patch_scalar_function = ScalarFunction("diff_patch", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                                 LogicalType::VARCHAR, DiffPatchScalarFun);
	diff_patch_scalar_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
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
		                   result.SetVectorType(VectorType::FLAT_VECTOR);
		                   auto out_data = FlatVector::GetData<string_t>(result);
		                   for (idx_t i = 0; i < args.size(); i++) {
			                   bool old_is_null = FlatVector::IsNull(old_col, i);
			                   bool new_is_null = FlatVector::IsNull(new_col, i);
			                   std::string js;
			                   if (new_is_null) {
				                   // new=NULL => treat as no change
				                   js = "[]";
			                   } else {
				                   std::string old_s = old_is_null
				                                           ? std::string()
				                                           : FlatVector::GetData<string_t>(old_col)[i].GetString();
				                   std::string new_s = FlatVector::GetData<string_t>(new_col)[i].GetString();
				                   js = diffpatch::GeneratePatchJson(old_s, new_s, false, nullptr);
			                   }
			                   out_data[i] = StringVector::AddString(result, js);
		                   }
	                   });
	// Handle NULLs manually to allow treating NULL as empty string
	make_patch_scalar_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
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
		// Treat NULL old/new as empty strings: handle NULLs in the function
		make_cols_text_fn.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
		ExtensionUtil::RegisterFunction(instance, make_cols_text_fn);
	}

	// Register apply_cols(old_content, ops, plus_concat, vals) -> VARCHAR
	{
		ScalarFunction apply_cols_fn(
		    "apply_cols",
		    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::BIGINT)},
		    LogicalType::VARCHAR, ApplyColsScalarFun);
		// We handle NULLs manually (old may be NULL but ops/plus/vals non-empty)
		apply_cols_fn.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
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
