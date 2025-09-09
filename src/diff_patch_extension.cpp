#define DUCKDB_EXTENSION_MAIN

#include "diff_patch_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
// Vector executors
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

// JSON parser (yyjson comes with DuckDB)
#include "yyjson.hpp"
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

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto diff_patch_scalar_function = ScalarFunction("diff_patch", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                                 LogicalType::VARCHAR, DiffPatchScalarFun);
	ExtensionUtil::RegisterFunction(instance, diff_patch_scalar_function);

	// Register patch_len(patch_json) -> BIGINT
	auto patch_len_scalar_function =
	    ScalarFunction("patch_len", {LogicalType::VARCHAR}, LogicalType::BIGINT, PatchLenScalarFun);
	ExtensionUtil::RegisterFunction(instance, patch_len_scalar_function);
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
