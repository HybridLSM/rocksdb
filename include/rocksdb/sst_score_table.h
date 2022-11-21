#pragma once


#include <unordered_map>
#include <cassert>
#include <mutex>
#include <shared_mutex>

namespace ROCKSDB_NAMESPACE {

	struct ScoreSst {
		int score;
		uint64_t sst_id;
	};

	class ScoreTable {
		public:

			explicit ScoreTable(){
				cur_highest.score = 0;
				cur_highest.sst_id = 0;
			}

			void AddScore(uint64_t sst_id) {
                std::unique_lock<std::shared_mutex> lock(mutex_);
				auto it = score_table.find(sst_id);
				if(it != score_table.end()) {
					score_table[sst_id] = it->second + 1;
				} else {
					score_table[sst_id] = 1;
				}
				if (score_table[sst_id] > cur_highest.score) {
					cur_highest.sst_id = sst_id;
					cur_highest.score = score_table[sst_id];
				}
			}

			void RemoveSstScore(uint64_t sst_id) {
                std::unique_lock<std::shared_mutex> lock(mutex_);
				score_table.erase(sst_id);
			}

			void ResetHighest() {
                std::unique_lock<std::shared_mutex> lock(mutex_);
				cur_highest.score = 0;
				for (auto it: score_table) {
					if (it.second > cur_highest.score) {
						cur_highest.score = it.second;
						cur_highest.sst_id = it.first;
					}
				}
			}

			bool Find(uint64_t sst_id) {
                std::shared_lock<std::shared_mutex> lock(mutex_);
				return score_table.find(sst_id) != score_table.end();
			}

			ScoreSst GetHighScoreSst() {
                std::shared_lock<std::shared_mutex> lock(mutex_);
				return cur_highest;
			}

			~ScoreTable(){}

		private:
            std::shared_mutex mutex_;
			std::unordered_map<uint64_t, int> score_table;
			ScoreSst cur_highest;
            

			
	};

}
