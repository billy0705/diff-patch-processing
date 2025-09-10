#include "include/diff_lib.hpp"

#include <unordered_map>
#include <unordered_set>
#include <tuple>

#include "yyjson.hpp"
using namespace duckdb_yyjson;

namespace diffpatch {

struct Match {
	int a;
	int b;
	int size;
};

// UTF-8 decoding to codepoints with byte offsets (sentinel at end)
static void Utf8ToCodepoints(const std::string &s, std::vector<uint32_t> &out, std::vector<size_t> &byte_offsets) {
	out.clear();
	byte_offsets.clear();
	size_t i = 0, n = s.size();
	while (i < n) {
		byte_offsets.push_back(i);
		unsigned char c = (unsigned char)s[i];
		uint32_t cp = 0;
		size_t clen = 1;
		if (c < 0x80) {
			cp = c;
			clen = 1;
		} else if ((c >> 5) == 0x6 && i + 1 < n) {
			unsigned char c1 = (unsigned char)s[i + 1];
			if ((c1 & 0xC0) == 0x80) {
				cp = ((c & 0x1F) << 6) | (c1 & 0x3F);
				clen = 2;
			} else {
				cp = c;
				clen = 1;
			}
		} else if ((c >> 4) == 0xE && i + 2 < n) {
			unsigned char c1 = (unsigned char)s[i + 1];
			unsigned char c2 = (unsigned char)s[i + 2];
			if (((c1 & 0xC0) == 0x80) && ((c2 & 0xC0) == 0x80)) {
				cp = ((c & 0x0F) << 12) | ((c1 & 0x3F) << 6) | (c2 & 0x3F);
				clen = 3;
			} else {
				cp = c;
				clen = 1;
			}
		} else if ((c >> 3) == 0x1E && i + 3 < n) {
			unsigned char c1 = (unsigned char)s[i + 1];
			unsigned char c2 = (unsigned char)s[i + 2];
			unsigned char c3 = (unsigned char)s[i + 3];
			if (((c1 & 0xC0) == 0x80) && ((c2 & 0xC0) == 0x80) && ((c3 & 0xC0) == 0x80)) {
				cp = ((c & 0x07) << 18) | ((c1 & 0x3F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
				clen = 4;
			} else {
				cp = c;
				clen = 1;
			}
		} else {
			cp = c;
			clen = 1; // invalid lead or truncated
		}
		out.push_back(cp);
		i += clen;
	}
	byte_offsets.push_back(n); // sentinel: end of string
}

class SequenceMatcher {
public:
	SequenceMatcher(const std::vector<uint32_t> &a, const std::vector<uint32_t> &b, bool autojunk = false,
	                const std::function<bool(uint32_t)> &isjunk = nullptr)
	    : a_(a), b_(b), autojunk_(autojunk), isjunk_(isjunk) {
		chain_b();
	}

	Match find_longest_match(int alo, int ahi, int blo, int bhi) {
		const auto &a = a_;
		const auto &b = b_;
		int besti = alo, bestj = blo, bestsize = 0;
		std::unordered_map<int, int> j2len;
		const std::vector<int> empty;
		for (int i = alo; i < ahi; ++i) {
			auto it = b2j_.find(a[i]);
			const std::vector<int> &js = (it == b2j_.end()) ? empty : it->second;
			std::unordered_map<int, int> newj2len;
			for (int j : js) {
				if (j < blo)
					continue;
				if (j >= bhi)
					break;
				int k = (j2len.count(j - 1) ? j2len[j - 1] : 0) + 1;
				newj2len[j] = k;
				if (k > bestsize) {
					besti = i - k + 1;
					bestj = j - k + 1;
					bestsize = k;
				}
			}
			j2len.swap(newj2len);
		}
		// Extend to include equal elements around (no junk filtering when isjunk_==nullptr)
		while (besti > alo && bestj > blo && a[besti - 1] == b[bestj - 1]) {
			--besti;
			--bestj;
			++bestsize;
		}
		while (besti + bestsize < ahi && bestj + bestsize < bhi && a[besti + bestsize] == b[bestj + bestsize]) {
			++bestsize;
		}
		return Match {besti, bestj, bestsize};
	}

	std::vector<Match> get_matching_blocks() {
		if (has_cached_blocks_)
			return cached_blocks_;
		int la = (int)a_.size();
		int lb = (int)b_.size();
		std::vector<std::tuple<int, int, int, int>> queue;
		queue.emplace_back(0, la, 0, lb);
		std::vector<Match> blocks;
		while (!queue.empty()) {
			std::tuple<int, int, int, int> quad = queue.back();
			int alo = std::get<0>(quad);
			int ahi = std::get<1>(quad);
			int blo = std::get<2>(quad);
			int bhi = std::get<3>(quad);
			queue.pop_back();
			Match m = find_longest_match(alo, ahi, blo, bhi);
			int i = m.a, j = m.b, k = m.size;
			if (k > 0) {
				blocks.push_back(m);
				if (alo < i && blo < j)
					queue.emplace_back(alo, i, blo, j);
				if (i + k < ahi && j + k < bhi)
					queue.emplace_back(i + k, ahi, j + k, bhi);
			}
		}
		std::sort(blocks.begin(), blocks.end(), [](const Match &x, const Match &y) {
			if (x.a != y.a)
				return x.a < y.a;
			return x.b < y.b;
		});
		// Collapse adjacent
		int i1 = 0, j1 = 0, k1 = 0;
		std::vector<Match> non_adjacent;
		for (auto &mb : blocks) {
			int i2 = mb.a, j2 = mb.b, k2 = mb.size;
			if (i1 + k1 == i2 && j1 + k1 == j2) {
				k1 += k2;
			} else {
				if (k1)
					non_adjacent.push_back(Match {i1, j1, k1});
				i1 = i2;
				j1 = j2;
				k1 = k2;
			}
		}
		if (k1)
			non_adjacent.push_back(Match {i1, j1, k1});
		non_adjacent.push_back(Match {la, lb, 0});
		cached_blocks_ = non_adjacent;
		has_cached_blocks_ = true;
		return cached_blocks_;
	}

	struct Opcode {
		std::string tag;
		int i1, i2, j1, j2;
	};
	std::vector<Opcode> get_opcodes() {
		std::vector<Match> mbs = get_matching_blocks();
		std::vector<Opcode> ops;
		int i = 0, j = 0;
		for (const auto &mb : mbs) {
			int ai = mb.a, bj = mb.b, size = mb.size;
			std::string tag;
			if (i < ai && j < bj)
				tag = "replace";
			else if (i < ai)
				tag = "delete";
			else if (j < bj)
				tag = "insert";
			if (!tag.empty())
				ops.push_back(Opcode {tag, i, ai, j, bj});
			i = ai + size;
			j = bj + size;
			if (size)
				ops.push_back(Opcode {"equal", ai, i, bj, j});
		}
		return ops;
	}

private:
	void chain_b() {
		b2j_.clear();
		std::unordered_map<uint32_t, int> counts;
		if (autojunk_ && (int)b_.size() >= 200) {
			for (auto ch : b_)
				counts[ch]++;
		}
		int n = (int)b_.size();
		int threshold = n / 100 + 1;
		for (int i = 0; i < n; ++i) {
			uint32_t elt = b_[i];
			if (isjunk_ && isjunk_(elt))
				continue;
			if (autojunk_ && n >= 200) {
				auto itc = counts.find(elt);
				if (itc != counts.end() && itc->second > threshold)
					continue; // popular
			}
			b2j_[elt].push_back(i);
		}
	}

	const std::vector<uint32_t> &a_;
	const std::vector<uint32_t> &b_;
	bool autojunk_;
	std::function<bool(uint32_t)> isjunk_;
	std::unordered_map<uint32_t, std::vector<int>> b2j_;
	std::vector<Match> cached_blocks_;
	bool has_cached_blocks_ = false;
};

std::string GeneratePatchJson(const std::string &old_utf8, const std::string &neu_utf8, bool autojunk,
                              const std::function<bool(uint32_t)> &isjunk) {
	std::vector<uint32_t> a, b;
	std::vector<size_t> a_off, b_off;
	Utf8ToCodepoints(old_utf8, a, a_off);
	Utf8ToCodepoints(neu_utf8, b, b_off);

	SequenceMatcher sm(a, b, autojunk, isjunk);
	auto ops = sm.get_opcodes();

	yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
	yyjson_mut_val *root = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, root);

	for (const auto &op : ops) {
		if (op.tag == "equal") {
			yyjson_mut_val *arr = yyjson_mut_arr_add_arr(doc, root);
			yyjson_mut_arr_add_str(doc, arr, "=");
			yyjson_mut_arr_add_int(doc, arr, (int64_t)(op.i2 - op.i1));
		} else if (op.tag == "insert") {
			yyjson_mut_val *arr = yyjson_mut_arr_add_arr(doc, root);
			yyjson_mut_arr_add_str(doc, arr, "+");
			size_t s = (size_t)b_off[op.j1];
			size_t e = (size_t)b_off[op.j2];
			std::string sub = neu_utf8.substr(s, e - s);
			// Use strcpy variant to avoid any accidental binary content issues
			yyjson_mut_arr_add_strcpy(doc, arr, sub.c_str());
		} else if (op.tag == "delete") {
			yyjson_mut_val *arr = yyjson_mut_arr_add_arr(doc, root);
			yyjson_mut_arr_add_str(doc, arr, "-");
			yyjson_mut_arr_add_int(doc, arr, (int64_t)(op.i2 - op.i1));
		} else if (op.tag == "replace") {
			if (op.i2 > op.i1) {
				yyjson_mut_val *arr = yyjson_mut_arr_add_arr(doc, root);
				yyjson_mut_arr_add_str(doc, arr, "-");
				yyjson_mut_arr_add_int(doc, arr, (int64_t)(op.i2 - op.i1));
			}
			if (op.j2 > op.j1) {
				yyjson_mut_val *arr = yyjson_mut_arr_add_arr(doc, root);
				yyjson_mut_arr_add_str(doc, arr, "+");
				size_t s = (size_t)b_off[op.j1];
				size_t e = (size_t)b_off[op.j2];
				std::string sub = neu_utf8.substr(s, e - s);
				yyjson_mut_arr_add_strcpy(doc, arr, sub.c_str());
			}
		}
	}

	size_t out_len = 0;
	char *out = yyjson_mut_write(doc, 0, &out_len);
	std::string result = out ? std::string(out, out_len) : std::string("[]");
	if (out)
		free(out);
	yyjson_mut_doc_free(doc);
	return result;
}

} // namespace diffpatch
