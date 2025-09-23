// Shared Patch JSON parsing utilities
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "yyjson.hpp"

namespace patchjson {

struct PatchOp {
	// tag is one of '=', '+', '-'; unknown tags are ignored by the parser
	char tag = 0;
	// count is used for '=' and '-'
	int64_t count = 0;
	// insert is used for '+' (stringified if JSON value was numeric/real)
	std::string insert;
};

// Parse a JSON patch like [["=",5],["+","foo"],["-",2],...]
// Returns true on success (even if empty), false on parse error
inline bool ParsePatchJSON(const char *data, size_t len, std::vector<PatchOp> &out_ops) {
	using namespace duckdb_yyjson;
	out_ops.clear();
	yyjson_doc *doc = yyjson_read(data, len, 0);
	if (!doc) {
		return false;
	}
	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_arr(root)) {
		yyjson_doc_free(doc);
		return false;
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
		PatchOp op;
		op.tag = tag[0];
		if (op.tag == '=') {
			long long n = 0;
			if (yyjson_is_int(val_val)) {
				n = yyjson_get_int(val_val);
			} else if (yyjson_is_str(val_val)) {
				const char *s = yyjson_get_str(val_val);
				n = s ? atoll(s) : 0;
			} else if (yyjson_is_real(val_val)) {
				// real to int (truncate)
				n = (long long)yyjson_get_real(val_val);
			}
			if (n < 0)
				n = 0;
			op.count = (int64_t)n;
			out_ops.push_back(std::move(op));
		} else if (op.tag == '-') {
			long long n = 0;
			if (yyjson_is_int(val_val)) {
				n = yyjson_get_int(val_val);
			} else if (yyjson_is_str(val_val)) {
				const char *s = yyjson_get_str(val_val);
				n = s ? atoll(s) : 0;
			} else if (yyjson_is_real(val_val)) {
				n = (long long)yyjson_get_real(val_val);
			}
			if (n < 0)
				n = 0;
			op.count = (int64_t)n;
			out_ops.push_back(std::move(op));
		} else if (op.tag == '+') {
			if (yyjson_is_str(val_val)) {
				const char *s = yyjson_get_str(val_val);
				if (s)
					op.insert.assign(s, yyjson_get_len(val_val));
			} else if (yyjson_is_int(val_val)) {
				char buf[32];
				int l = snprintf(buf, sizeof(buf), "%lld", (long long)yyjson_get_int(val_val));
				if (l > 0)
					op.insert.assign(buf, (size_t)l);
			} else if (yyjson_is_real(val_val)) {
				char buf[64];
				int l = snprintf(buf, sizeof(buf), "%g", yyjson_get_real(val_val));
				if (l > 0)
					op.insert.assign(buf, (size_t)l);
			}
			out_ops.push_back(std::move(op));
		} else {
			// ignore unknown op
		}
	}
	yyjson_doc_free(doc);
	return true;
}

// Serialize patch operations into the compact JSON form used by the extension.
inline std::string SerializePatchJSON(const std::vector<PatchOp> &ops) {
	using namespace duckdb_yyjson;
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, root);

	for (const auto &op : ops) {
		yyjson_mut_val *arr = yyjson_mut_arr_add_arr(doc, root);
		yyjson_mut_arr_add_strn(doc, arr, &op.tag, 1);
		if (op.tag == '+') {
			yyjson_mut_arr_add_strcpy(doc, arr, op.insert.c_str());
		} else {
			yyjson_mut_arr_add_int(doc, arr, op.count);
		}
	}

	size_t out_len = 0;
	char *raw = yyjson_mut_write(doc, 0, &out_len);
	std::string result = raw ? std::string(raw, out_len) : std::string("[]");
	if (raw) {
		free(raw);
	}
	yyjson_mut_doc_free(doc);
	return result;
}

inline bool ParsePatchJSON(const std::string &json, std::vector<PatchOp> &out_ops) {
	return ParsePatchJSON(json.c_str(), json.size(), out_ops);
}

} // namespace patchjson
