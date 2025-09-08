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
				    size_t take = (size_t)n;
				    if (idx > old_str.size())
					    idx = old_str.size();
				    size_t remain = old_str.size() - idx;
				    if (take > remain)
					    take = remain;
				    out.append(old_str.data() + idx, take);
				    idx += take;
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
				    size_t skip = (size_t)n;
				    if (idx > old_str.size())
					    idx = old_str.size();
				    size_t remain = old_str.size() - idx;
				    if (skip > remain)
					    skip = remain;
				    idx += skip;
			    } else {
				    // ignore unknown op
			    }
		    }

		    yyjson_doc_free(doc);
		    return StringVector::AddString(result, out);
	    });
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto diff_patch_scalar_function = ScalarFunction("diff_patch", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                                 LogicalType::VARCHAR, DiffPatchScalarFun);
	ExtensionUtil::RegisterFunction(instance, diff_patch_scalar_function);
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
