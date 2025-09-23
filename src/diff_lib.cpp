#include "include/diff_lib.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cwctype>
#include <deque>
#include <functional>
#include <list>
#include <iterator>
#include <string>
#include <unordered_map>
#include <vector>

#include "yyjson.hpp"
#include "include/utf8_utils.hpp"
#include "include/patch_json_utils.hpp"
using namespace duckdb_yyjson;

namespace diffpatch {

enum class Operation { Delete, Insert, Equal };

struct Diff {
	Operation operation;
	std::u32string text;

	Diff() = default;
	Diff(Operation op, const std::u32string &txt) : operation(op), text(txt) {
	}
};

// Utility structure used during the line-mode speedup pass.
struct LinesToCharsResult {
	std::u32string chars1;
	std::u32string chars2;
	std::vector<std::u32string> line_array;
};

// Lightweight port of Google's diff-match-patch tailored for UTF-32 inputs.
// Public callers only need the `diff_main` overloads; everything else is
// internal to the algorithm.
class DiffMatchPatch {
public:
	float Diff_Timeout = 1.0f;
	short Diff_EditCost = 4;
	float Match_Threshold = 0.5f;
	int Match_Distance = 1000;
	float Patch_DeleteThreshold = 0.5f;
	short Patch_Margin = 4;
	short Match_MaxBits = 32;

	using DiffList = std::list<Diff>;

	DiffList diff_main(const std::u32string &text1, const std::u32string &text2) {
		return diff_main(text1, text2, true);
	}

	DiffList diff_main(const std::u32string &text1, const std::u32string &text2, bool checklines) {
		auto deadline = Diff_Timeout <= 0 ? std::chrono::steady_clock::time_point::max()
		                                  : std::chrono::steady_clock::now() +
		                                        std::chrono::milliseconds(static_cast<int64_t>(Diff_Timeout * 1000));
		return diff_main(text1, text2, checklines, deadline);
	}

private:
	DiffList diff_main(const std::u32string &text1, const std::u32string &text2, bool checklines,
	                   std::chrono::steady_clock::time_point deadline) {
		if (&text1 == &text2) {
			DiffList diffs;
			if (!text1.empty()) {
				diffs.emplace_back(Operation::Equal, text1);
			}
			return diffs;
		}
		if (text1.empty() && text2.empty()) {
			return DiffList();
		}
		if (text1 == text2) {
			DiffList diffs;
			diffs.emplace_back(Operation::Equal, text1);
			return diffs;
		}

		int commonlength = diff_commonPrefix(text1, text2);
		std::u32string commonprefix = text1.substr(0, commonlength);
		std::u32string txt1 = text1.substr(commonlength);
		std::u32string txt2 = text2.substr(commonlength);

		commonlength = diff_commonSuffix(txt1, txt2);
		std::u32string commonsuffix = txt1.substr(txt1.size() - commonlength);
		txt1 = txt1.substr(0, txt1.size() - commonlength);
		txt2 = txt2.substr(0, txt2.size() - commonlength);

		DiffList diffs = diff_compute(txt1, txt2, checklines, deadline);

		if (!commonprefix.empty()) {
			diffs.emplace_front(Operation::Equal, commonprefix);
		}
		if (!commonsuffix.empty()) {
			diffs.emplace_back(Operation::Equal, commonsuffix);
		}

		return diffs;
	}

	DiffList diff_compute(const std::u32string &text1, const std::u32string &text2, bool checklines,
	                      std::chrono::steady_clock::time_point deadline) {
		DiffList diffs;
		if (text1.empty()) {
			if (!text2.empty()) {
				diffs.emplace_back(Operation::Insert, text2);
			}
			return diffs;
		}
		if (text2.empty()) {
			if (!text1.empty()) {
				diffs.emplace_back(Operation::Delete, text1);
			}
			return diffs;
		}

		const std::u32string &longtext = text1.size() > text2.size() ? text1 : text2;
		const std::u32string &shorttext = text1.size() > text2.size() ? text2 : text1;
		size_t i = longtext.find(shorttext);
		if (i != std::u32string::npos) {
			Operation op = (text1.size() > text2.size()) ? Operation::Delete : Operation::Insert;
			if (i != 0) {
				diffs.emplace_back(op, longtext.substr(0, i));
			}
			diffs.emplace_back(Operation::Equal, shorttext);
			if (i + shorttext.size() < longtext.size()) {
				diffs.emplace_back(op, longtext.substr(i + shorttext.size()));
			}
			return diffs;
		}

		if (shorttext.size() == 1) {
			diffs.emplace_back(Operation::Delete, text1);
			diffs.emplace_back(Operation::Insert, text2);
			return diffs;
		}

		std::array<std::u32string, 5> hm;
		if (diff_halfMatch(text1, text2, hm)) {
			DiffList diffs_a = diff_main(hm[0], hm[2], checklines, deadline);
			DiffList diffs_b = diff_main(hm[1], hm[3], checklines, deadline);
			diffs.splice(diffs.end(), diffs_a);
			diffs.emplace_back(Operation::Equal, hm[4]);
			diffs.splice(diffs.end(), diffs_b);
			return diffs;
		}

		if (checklines && text1.size() > 100 && text2.size() > 100) {
			return diff_lineMode(text1, text2, deadline);
		}

		return diff_bisect(text1, text2, deadline);
	}

	DiffList diff_lineMode(const std::u32string &text1, const std::u32string &text2,
	                       std::chrono::steady_clock::time_point deadline) {
		LinesToCharsResult a = diff_linesToChars(text1, text2);
		DiffList diffs = diff_main(a.chars1, a.chars2, false, deadline);
		diff_charsToLines(diffs, a.line_array);

		diffs.emplace_back(Operation::Equal, std::u32string());
		auto it = diffs.begin();
		int count_delete = 0;
		int count_insert = 0;
		std::u32string text_delete;
		std::u32string text_insert;
		while (it != diffs.end()) {
			if (it->operation == Operation::Insert) {
				count_insert++;
				text_insert += it->text;
			} else if (it->operation == Operation::Delete) {
				count_delete++;
				text_delete += it->text;
			} else {
				if (count_delete != 0 && count_insert != 0) {
					auto prev = it;
					for (int j = 0; j < count_delete + count_insert; ++j) {
						--prev;
					}
					for (int j = 0; j < count_delete + count_insert; ++j) {
						prev = diffs.erase(prev);
					}
					DiffList subDiffs = diff_main(text_delete, text_insert, false, deadline);
					for (auto &d : subDiffs) {
						prev = diffs.insert(prev, d);
						++prev;
					}
					it = prev;
				}
				count_insert = 0;
				count_delete = 0;
				text_delete.clear();
				text_insert.clear();
			}
			++it;
		}
		diffs.pop_back();
		return diffs;
	}

	DiffList diff_bisect(const std::u32string &text1, const std::u32string &text2,
	                     std::chrono::steady_clock::time_point deadline) {
		int text1_length = static_cast<int>(text1.size());
		int text2_length = static_cast<int>(text2.size());
		int max_d = (text1_length + text2_length + 1) / 2;
		int v_offset = max_d;
		int v_length = 2 * max_d;
		std::vector<int> v1(v_length, -1);
		std::vector<int> v2(v_length, -1);
		v1[v_offset + 1] = 0;
		v2[v_offset + 1] = 0;
		int delta = text1_length - text2_length;
		bool front = (delta % 2 != 0);
		int k1start = 0;
		int k1end = 0;
		int k2start = 0;
		int k2end = 0;
		for (int d = 0; d < max_d; ++d) {
			if (std::chrono::steady_clock::now() > deadline) {
				break;
			}

			for (int k1 = -d + k1start; k1 <= d - k1end; k1 += 2) {
				int k1_offset = v_offset + k1;
				int x1;
				if (k1 == -d || (k1 != d && v1[k1_offset - 1] < v1[k1_offset + 1])) {
					x1 = v1[k1_offset + 1];
				} else {
					x1 = v1[k1_offset - 1] + 1;
				}
				int y1 = x1 - k1;
				while (x1 < text1_length && y1 < text2_length && text1[x1] == text2[y1]) {
					++x1;
					++y1;
				}
				v1[k1_offset] = x1;
				if (x1 > text1_length) {
					k1end += 2;
				} else if (y1 > text2_length) {
					k1start += 2;
				} else if (front) {
					int k2_offset = v_offset + delta - k1;
					if (k2_offset >= 0 && k2_offset < v_length && v2[k2_offset] != -1) {
						int x2 = text1_length - v2[k2_offset];
						if (x1 >= x2) {
							return diff_bisectSplit(text1, text2, x1, y1, deadline);
						}
					}
				}
			}

			for (int k2 = -d + k2start; k2 <= d - k2end; k2 += 2) {
				int k2_offset = v_offset + k2;
				int x2;
				if (k2 == -d || (k2 != d && v2[k2_offset - 1] < v2[k2_offset + 1])) {
					x2 = v2[k2_offset + 1];
				} else {
					x2 = v2[k2_offset - 1] + 1;
				}
				int y2 = x2 - k2;
				while (x2 < text1_length && y2 < text2_length &&
				       text1[text1_length - x2 - 1] == text2[text2_length - y2 - 1]) {
					++x2;
					++y2;
				}
				v2[k2_offset] = x2;
				if (x2 > text1_length) {
					k2end += 2;
				} else if (y2 > text2_length) {
					k2start += 2;
				} else if (!front) {
					int k1_offset = v_offset + delta - k2;
					if (k1_offset >= 0 && k1_offset < v_length && v1[k1_offset] != -1) {
						int x1 = v1[k1_offset];
						int y1 = v_offset + x1 - k1_offset;
						x2 = text1_length - x2;
						y2 = text2_length - y2;
						if (x1 >= x2) {
							return diff_bisectSplit(text1, text2, x1, y1, deadline);
						}
					}
				}
			}
		}

		DiffList diffs;
		diffs.emplace_back(Operation::Delete, text1);
		diffs.emplace_back(Operation::Insert, text2);
		return diffs;
	}

	DiffList diff_bisectSplit(const std::u32string &text1, const std::u32string &text2, int x, int y,
	                          std::chrono::steady_clock::time_point deadline) {
		std::u32string text1a = text1.substr(0, x);
		std::u32string text2a = text2.substr(0, y);
		std::u32string text1b = text1.substr(x);
		std::u32string text2b = text2.substr(y);

		DiffList diffs = diff_main(text1a, text2a, false, deadline);
		DiffList diffsb = diff_main(text1b, text2b, false, deadline);
		diffs.splice(diffs.end(), diffsb);
		return diffs;
	}

	LinesToCharsResult diff_linesToChars(const std::u32string &text1, const std::u32string &text2) {
		std::vector<std::u32string> line_array;
		line_array.emplace_back();
		std::unordered_map<std::u32string, size_t> line_hash;
		std::u32string chars1 = diff_linesToCharsMunge(text1, line_array, line_hash, 40000);
		std::u32string chars2 = diff_linesToCharsMunge(text2, line_array, line_hash, 65535);
		return {chars1, chars2, line_array};
	}

	std::u32string diff_linesToCharsMunge(const std::u32string &text, std::vector<std::u32string> &line_array,
	                                      std::unordered_map<std::u32string, size_t> &line_hash, size_t max_lines) {
		size_t line_start = 0;
		size_t line_end = 0;
		std::u32string line;
		std::u32string chars;
		while (line_end < text.size()) {
			size_t pos = text.find(U'\n', line_start);
			if (pos == std::u32string::npos) {
				pos = text.size() - 1;
			} else {
				line_end = pos;
			}
			line = text.substr(line_start, pos - line_start + 1);
			auto it = line_hash.find(line);
			if (it != line_hash.end()) {
				chars.push_back(static_cast<char32_t>(it->second));
			} else {
				if (line_array.size() == max_lines) {
					line = text.substr(line_start);
					line_end = text.size();
				}
				line_array.push_back(line);
				size_t index = line_array.size() - 1;
				line_hash.emplace(line, index);
				chars.push_back(static_cast<char32_t>(index));
			}
			line_start = pos + 1;
			line_end = line_start;
			if (line_start >= text.size()) {
				break;
			}
		}
		return chars;
	}

	void diff_charsToLines(DiffList &diffs, const std::vector<std::u32string> &line_array) {
		for (auto &diff : diffs) {
			std::u32string text;
			text.reserve(diff.text.size() * 2);
			for (char32_t ch : diff.text) {
				size_t index = static_cast<size_t>(ch);
				if (index < line_array.size()) {
					text += line_array[index];
				}
			}
			diff.text = std::move(text);
		}
	}

	int diff_commonPrefix(const std::u32string &text1, const std::u32string &text2) {
		int n = static_cast<int>(std::min(text1.size(), text2.size()));
		for (int i = 0; i < n; ++i) {
			if (text1[i] != text2[i]) {
				return i;
			}
		}
		return n;
	}

	int diff_commonSuffix(const std::u32string &text1, const std::u32string &text2) {
		int text1_length = static_cast<int>(text1.size());
		int text2_length = static_cast<int>(text2.size());
		int n = std::min(text1_length, text2_length);
		for (int i = 1; i <= n; ++i) {
			if (text1[text1_length - i] != text2[text2_length - i]) {
				return i - 1;
			}
		}
		return n;
	}

	int diff_commonOverlap(const std::u32string &text1, const std::u32string &text2) {
		int text1_length = static_cast<int>(text1.size());
		int text2_length = static_cast<int>(text2.size());
		if (text1_length == 0 || text2_length == 0) {
			return 0;
		}
		if (text1_length > text2_length) {
			return diff_commonOverlap(text1.substr(text1_length - text2_length), text2);
		}
		if (text1_length < text2_length) {
			return diff_commonOverlap(text1, text2.substr(0, text1_length));
		}
		int text_length = std::min(text1_length, text2_length);
		if (text1 == text2) {
			return text_length;
		}
		int best = 0;
		int length = 1;
		while (length <= text_length) {
			std::u32string pattern = text1.substr(text_length - length);
			size_t found = text2.find(pattern);
			if (found == std::u32string::npos) {
				return best;
			}
			length += static_cast<int>(found);
			if (length > text_length) {
				length = text_length;
			}
			if (found == 0 || text1.substr(text_length - length) == text2.substr(0, length)) {
				best = length;
				++length;
			}
		}
		return best;
	}

	bool diff_halfMatch(const std::u32string &text1, const std::u32string &text2, std::array<std::u32string, 5> &out) {
		if (Diff_Timeout <= 0) {
			return false;
		}
		const std::u32string &longtext = text1.size() > text2.size() ? text1 : text2;
		const std::u32string &shorttext = text1.size() > text2.size() ? text2 : text1;
		if (longtext.size() < 4 || shorttext.size() * 2 < longtext.size()) {
			return false;
		}
		std::array<std::u32string, 5> hm1;
		std::array<std::u32string, 5> hm2;
		bool has_hm1 = diff_halfMatchI(longtext, shorttext, (longtext.size() + 3) / 4, hm1);
		bool has_hm2 = diff_halfMatchI(longtext, shorttext, (longtext.size() + 1) / 2, hm2);
		std::array<std::u32string, 5> hm;
		if (!has_hm1 && !has_hm2) {
			return false;
		} else if (has_hm1 && !has_hm2) {
			hm = hm1;
		} else if (!has_hm1 && has_hm2) {
			hm = hm2;
		} else {
			hm = (hm1[4].size() > hm2[4].size()) ? hm1 : hm2;
		}
		if (text1.size() > text2.size()) {
			out = hm;
		} else {
			out[0] = hm[2];
			out[1] = hm[3];
			out[2] = hm[0];
			out[3] = hm[1];
			out[4] = hm[4];
		}
		return true;
	}

	bool diff_halfMatchI(const std::u32string &longtext, const std::u32string &shorttext, size_t i,
	                     std::array<std::u32string, 5> &out) {
		if (i >= longtext.size()) {
			return false;
		}
		size_t seed_length = longtext.size() / 4;
		std::u32string seed = longtext.substr(i, seed_length);
		size_t j = shorttext.find(seed);
		if (j == std::u32string::npos) {
			return false;
		}
		std::u32string best_common;
		std::u32string best_long_a, best_long_b;
		std::u32string best_short_a, best_short_b;
		while (j != std::u32string::npos) {
			int prefix_length = diff_commonPrefix(longtext.substr(i), shorttext.substr(j));
			int suffix_length = diff_commonSuffix(longtext.substr(0, i), shorttext.substr(0, j));
			if (best_common.size() < static_cast<size_t>(suffix_length + prefix_length)) {
				best_common = shorttext.substr(j - suffix_length, suffix_length + prefix_length);
				best_long_a = longtext.substr(0, i - suffix_length);
				best_long_b = longtext.substr(i + prefix_length);
				best_short_a = shorttext.substr(0, j - suffix_length);
				best_short_b = shorttext.substr(j + prefix_length);
			}
			j = shorttext.find(seed, j + 1);
		}
		if (best_common.size() * 2 >= longtext.size()) {
			out[0] = best_long_a;
			out[1] = best_long_b;
			out[2] = best_short_a;
			out[3] = best_short_b;
			out[4] = best_common;
			return true;
		}
		return false;
	}

	void diff_cleanupSemantic(DiffList &diffs) {
		if (diffs.empty()) {
			return;
		}
		bool changes = false;
		std::deque<typename DiffList::iterator> equalities;
		std::u32string last_equality;
		auto pointer = diffs.begin();
		int length_insertions1 = 0;
		int length_deletions1 = 0;
		int length_insertions2 = 0;
		int length_deletions2 = 0;
		while (pointer != diffs.end()) {
			if (pointer->operation == Operation::Equal) {
				equalities.push_back(pointer);
				length_insertions1 = length_insertions2;
				length_deletions1 = length_deletions2;
				length_insertions2 = 0;
				length_deletions2 = 0;
				last_equality = pointer->text;
			} else {
				if (pointer->operation == Operation::Insert) {
					length_insertions2 += static_cast<int>(pointer->text.size());
				} else {
					length_deletions2 += static_cast<int>(pointer->text.size());
				}
				if (!last_equality.empty() &&
				    last_equality.size() <= static_cast<size_t>(std::max(length_insertions1, length_deletions1)) &&
				    last_equality.size() <= static_cast<size_t>(std::max(length_insertions2, length_deletions2))) {
					if (!equalities.empty()) {
						pointer = equalities.back();
						equalities.pop_back();
					}
					pointer->operation = Operation::Delete;
					auto insert_it = diffs.insert(std::next(pointer), Diff(Operation::Insert, last_equality));
					length_insertions1 = 0;
					length_insertions2 = 0;
					length_deletions1 = 0;
					length_deletions2 = 0;
					last_equality.clear();
					if (!equalities.empty()) {
						equalities.clear();
					}
					pointer = insert_it;
					changes = true;
				}
			}
			++pointer;
		}

		if (changes) {
			diff_cleanupMerge(diffs);
		}
		diff_cleanupSemanticLossless(diffs);

		auto prev = diffs.begin();
		auto curr = diffs.begin();
		if (curr != diffs.end()) {
			++curr;
		}
		while (curr != diffs.end()) {
			if (prev->operation == Operation::Delete && curr->operation == Operation::Insert) {
				const std::u32string &deletion = prev->text;
				const std::u32string &insertion = curr->text;
				int overlap1 = diff_commonOverlap(deletion, insertion);
				int overlap2 = diff_commonOverlap(insertion, deletion);
				if (overlap1 >= overlap2) {
					if (overlap1 * 2 >= static_cast<int>(deletion.size()) ||
					    overlap1 * 2 >= static_cast<int>(insertion.size())) {
						std::u32string common = insertion.substr(0, overlap1);
						diffs.insert(curr, Diff(Operation::Equal, common));
						prev->text = deletion.substr(0, deletion.size() - overlap1);
						curr->text = insertion.substr(overlap1);
						prev = curr;
						curr = std::next(curr);
						changes = true;
					}
				} else {
					if (overlap2 * 2 >= static_cast<int>(deletion.size()) ||
					    overlap2 * 2 >= static_cast<int>(insertion.size())) {
						std::u32string common = deletion.substr(0, overlap2);
						diffs.insert(curr, Diff(Operation::Equal, common));
						prev->operation = Operation::Insert;
						prev->text = insertion.substr(0, insertion.size() - overlap2);
						curr->operation = Operation::Delete;
						curr->text = deletion.substr(overlap2);
						prev = curr;
						curr = std::next(curr);
						changes = true;
					}
				}
			}
			prev = curr;
			if (curr != diffs.end()) {
				++curr;
			}
		}
	}

	void diff_cleanupSemanticLossless(DiffList &) {
	}

	int diff_cleanupSemanticScore(const std::u32string &one, const std::u32string &two) {
		if (one.empty() || two.empty()) {
			return 6;
		}
		char32_t char1 = one.back();
		char32_t char2 = two.front();
		bool nonAlphaNumeric1 = !std::iswalnum(static_cast<wint_t>(char1));
		bool nonAlphaNumeric2 = !std::iswalnum(static_cast<wint_t>(char2));
		bool whitespace1 = nonAlphaNumeric1 && std::iswspace(static_cast<wint_t>(char1));
		bool whitespace2 = nonAlphaNumeric2 && std::iswspace(static_cast<wint_t>(char2));
		bool lineBreak1 = whitespace1 && (char1 == U'\n' || char1 == U'\r');
		bool lineBreak2 = whitespace2 && (char2 == U'\n' || char2 == U'\r');
		bool blankLine1 = lineBreak1 && endsWithBlankLine(one);
		bool blankLine2 = lineBreak2 && startsWithBlankLine(two);

		if (blankLine1 || blankLine2) {
			return 5;
		}
		if (lineBreak1 || lineBreak2) {
			return 4;
		}
		if (nonAlphaNumeric1 && !whitespace1 && whitespace2) {
			return 3;
		}
		if (whitespace1 || whitespace2) {
			return 2;
		}
		if (nonAlphaNumeric1 || nonAlphaNumeric2) {
			return 1;
		}
		return 0;
	}

	bool endsWithBlankLine(const std::u32string &text) {
		if (text.size() < 2) {
			return false;
		}
		size_t len = text.size();
		if (text[len - 1] != U'\n') {
			return false;
		}
		if (len >= 2 && text[len - 2] == U'\n') {
			return true;
		}
		if (len >= 3 && text[len - 2] == U'\r' && text[len - 3] == U'\n') {
			return true;
		}
		return false;
	}

	bool startsWithBlankLine(const std::u32string &text) {
		if (text.size() < 2) {
			return false;
		}
		if (text[0] != U'\n') {
			return false;
		}
		if (text.size() >= 2 && text[1] == U'\n') {
			return true;
		}
		if (text.size() >= 3 && text[1] == U'\r' && text[2] == U'\n') {
			return true;
		}
		return false;
	}

	void diff_cleanupEfficiency(DiffList &diffs) {
		if (diffs.empty()) {
			return;
		}
		bool changes = false;
		std::deque<typename DiffList::iterator> equalities;
		std::u32string last_equality;
		bool pre_ins = false;
		bool pre_del = false;
		bool post_ins = false;
		bool post_del = false;
		auto pointer = diffs.begin();
		auto safe_diff = pointer;
		while (pointer != diffs.end()) {
			if (pointer->operation == Operation::Equal) {
				if (pointer->text.size() < static_cast<size_t>(Diff_EditCost) && (post_ins || post_del)) {
					equalities.push_back(pointer);
					pre_ins = post_ins;
					pre_del = post_del;
					last_equality = pointer->text;
				} else {
					equalities.clear();
					last_equality.clear();
					safe_diff = pointer;
				}
				post_ins = false;
				post_del = false;
			} else {
				if (pointer->operation == Operation::Delete) {
					post_del = true;
				} else {
					post_ins = true;
				}
				if (!last_equality.empty() &&
				    ((pre_ins && pre_del && post_ins && post_del) ||
				     (last_equality.size() < static_cast<size_t>(Diff_EditCost / 2) &&
				      ((pre_ins ? 1 : 0) + (pre_del ? 1 : 0) + (post_ins ? 1 : 0) + (post_del ? 1 : 0) == 3))) &&
				    !equalities.empty()) {
					auto last_eq = equalities.back();
					while (pointer != last_eq) {
						pointer = std::prev(pointer);
					}
					pointer = last_eq;
					pointer->operation = Operation::Delete;
					pointer = diffs.insert(std::next(pointer), Diff(Operation::Insert, last_equality));
					equalities.pop_back();
					if (pre_ins && pre_del) {
						post_ins = true;
						post_del = true;
						equalities.clear();
						safe_diff = pointer;
					} else {
						if (!equalities.empty()) {
							equalities.pop_back();
						}
						if (equalities.empty()) {
							pointer = safe_diff;
						} else {
							pointer = equalities.back();
						}
						post_ins = false;
						post_del = false;
					}
					changes = true;
				}
			}
			++pointer;
		}
		if (changes) {
			diff_cleanupMerge(diffs);
		}
	}

	void diff_cleanupMerge(DiffList &diffs) {
		// Not used currently; kept for completeness.
		diffs.emplace_back(Operation::Equal, std::u32string());
		auto pointer = diffs.begin();
		int count_delete = 0;
		int count_insert = 0;
		std::u32string text_delete;
		std::u32string text_insert;
		auto prev_equal = diffs.end();
		while (pointer != diffs.end()) {
			if (pointer->operation == Operation::Insert) {
				count_insert++;
				text_insert += pointer->text;
				prev_equal = diffs.end();
			} else if (pointer->operation == Operation::Delete) {
				count_delete++;
				text_delete += pointer->text;
				prev_equal = diffs.end();
			} else {
				if (count_delete + count_insert > 1) {
					for (int i = 0; i < count_delete + count_insert; ++i) {
						pointer = std::prev(pointer);
					}
					for (int i = 0; i < count_delete; ++i) {
						pointer = diffs.erase(pointer);
					}
					for (int i = 0; i < count_insert; ++i) {
						pointer = diffs.erase(pointer);
					}
					int commonlength = diff_commonPrefix(text_insert, text_delete);
					if (commonlength != 0) {
						pointer = diffs.insert(pointer, Diff(Operation::Equal, text_insert.substr(0, commonlength)));
						text_insert = text_insert.substr(commonlength);
						text_delete = text_delete.substr(commonlength);
					}
					commonlength = diff_commonSuffix(text_insert, text_delete);
					if (commonlength != 0) {
						auto next = std::next(pointer);
						next->text = text_insert.substr(text_insert.size() - commonlength) + next->text;
						text_insert = text_insert.substr(0, text_insert.size() - commonlength);
						text_delete = text_delete.substr(0, text_delete.size() - commonlength);
					}
					if (!text_delete.empty()) {
						pointer = diffs.insert(pointer, Diff(Operation::Delete, text_delete));
						++pointer;
					}
					if (!text_insert.empty()) {
						pointer = diffs.insert(pointer, Diff(Operation::Insert, text_insert));
						++pointer;
					}
					pointer = std::next(pointer);
				}
				count_insert = 0;
				count_delete = 0;
				text_delete.clear();
				text_insert.clear();
				if (prev_equal != diffs.end()) {
					prev_equal->text += pointer->text;
					pointer = diffs.erase(pointer);
					pointer = prev_equal;
				} else {
					prev_equal = pointer;
				}
			}
			++pointer;
		}
		if (!diffs.empty() && diffs.back().text.empty()) {
			diffs.pop_back();
		}

		bool changes = false;
		pointer = diffs.begin();
		auto prev = pointer;
		auto curr = pointer;
		if (curr != diffs.end()) {
			++curr;
		}
		auto next = curr;
		if (next != diffs.end()) {
			++next;
		}
		while (next != diffs.end()) {
			if (prev->operation == Operation::Equal && next->operation == Operation::Equal) {
				if (curr->text.size() >= prev->text.size() &&
				    curr->text.substr(curr->text.size() - prev->text.size()) == prev->text) {
					curr->text = prev->text + curr->text.substr(0, curr->text.size() - prev->text.size());
					next->text = prev->text + next->text;
					diffs.erase(prev);
					prev = curr;
					curr = next;
					next = std::next(next);
					changes = true;
				} else if (curr->text.size() >= next->text.size() &&
				           curr->text.substr(0, next->text.size()) == next->text) {
					prev->text += next->text;
					curr->text = curr->text.substr(next->text.size()) + next->text;
					next = diffs.erase(next);
					next = std::next(curr);
					changes = true;
				}
			}
			prev = curr;
			curr = next;
			if (next != diffs.end()) {
				next = std::next(next);
			}
		}
		if (changes) {
			diff_cleanupMerge(diffs);
		}
	}

	std::u32string diff_text1(const DiffList &diffs) const {
		std::u32string text;
		for (const auto &diff : diffs) {
			if (diff.operation != Operation::Insert) {
				text += diff.text;
			}
		}
		return text;
	}

	std::u32string diff_text2(const DiffList &diffs) const {
		std::u32string text;
		for (const auto &diff : diffs) {
			if (diff.operation != Operation::Delete) {
				text += diff.text;
			}
		}
		return text;
	}

	int diff_levenshtein(const DiffList &diffs) const {
		int levenshtein = 0;
		int insertions = 0;
		int deletions = 0;
		for (const auto &diff : diffs) {
			if (diff.operation == Operation::Insert) {
				insertions += static_cast<int>(diff.text.size());
			} else if (diff.operation == Operation::Delete) {
				deletions += static_cast<int>(diff.text.size());
			} else {
				levenshtein += std::max(insertions, deletions);
				insertions = 0;
				deletions = 0;
			}
		}
		levenshtein += std::max(insertions, deletions);
		return levenshtein;
	}
};

static std::u32string ToU32String(const std::vector<uint32_t> &codepoints) {
	std::u32string result;
	result.reserve(codepoints.size());
	for (auto cp : codepoints) {
		result.push_back(static_cast<char32_t>(cp));
	}
	return result;
}

// Entry point used by the extension. Produces the compact JSON representation
// that downstream SQL functions consume in order to materialize diff columns.
std::string GeneratePatchJson(const std::string &old_utf8, const std::string &neu_utf8, bool autojunk,
                              const std::function<bool(uint32_t)> &isjunk) {
	(void)autojunk;
	(void)isjunk;
	std::vector<uint32_t> a, b;
	std::vector<size_t> a_off, b_off;
	Utf8ToCodepoints(old_utf8, a, a_off);
	Utf8ToCodepoints(neu_utf8, b, b_off);

	DiffMatchPatch dmp;
	std::u32string a32 = CodepointsToU32String(a);
	std::u32string b32 = CodepointsToU32String(b);

	auto diffs = dmp.diff_main(a32, b32);
	std::vector<patchjson::PatchOp> patch_ops;
	patch_ops.reserve(diffs.size());

	size_t a_index = 0;
	size_t b_index = 0;
	for (const auto &diff : diffs) {
		// Translate diff segments into the patch JSON dialect while converting
		// code point lengths back to UTF-8 byte ranges as needed.
		size_t len = diff.text.size();
		switch (diff.operation) {
		case Operation::Equal:
			if (len > 0) {
				patchjson::PatchOp op;
				op.tag = '=';
				op.count = static_cast<int64_t>(len);
				patch_ops.push_back(std::move(op));
			}
			a_index += len;
			b_index += len;
			break;
		case Operation::Insert:
			if (len > 0) {
				patchjson::PatchOp op;
				op.tag = '+';
				size_t start = b_off[b_index];
				size_t end = b_off[b_index + len];
				op.insert = neu_utf8.substr(start, end - start);
				patch_ops.push_back(std::move(op));
			}
			b_index += len;
			break;
		case Operation::Delete:
			if (len > 0) {
				patchjson::PatchOp op;
				op.tag = '-';
				op.count = static_cast<int64_t>(len);
				patch_ops.push_back(std::move(op));
			}
			a_index += len;
			break;
		}
	}

	return patchjson::SerializePatchJSON(patch_ops);
}

} // namespace diffpatch
