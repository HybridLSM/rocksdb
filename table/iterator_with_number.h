#pragma once

#include "table/internal_iterator.h"
#include "util/coding.h"
#include <iostream>
#include <shared_mutex>

namespace ROCKSDB_NAMESPACE {

class InternalIteratorWithFileNumber : public InternalIterator {
	public:
		InternalIteratorWithFileNumber(const uint64_t file_number, InternalIterator* data_iter)
													: file_number_(file_number),
														data_iter_(data_iter) {
			std::unique_lock<std::shared_mutex> lock(mutex_);
			// key_buf = (char*)malloc( sizeof(char) * 256);
		}
		~InternalIteratorWithFileNumber() {
			std::unique_lock<std::shared_mutex> lock(mutex_);
			// free(key_buf);
			delete data_iter_; 
		} // need delete ???

		bool Valid() const override { 
			return data_iter_->Valid(); 
		}
    void Seek(const Slice& target) override { 
			std::unique_lock<std::shared_mutex> lock(mutex_);
			data_iter_->Seek(target); 
		}
		void SeekForPrev(const Slice& target) { 
			std::unique_lock<std::shared_mutex> lock(mutex_);
			data_iter_->SeekForPrev(target); 
		}
    void SeekToFirst() override { 
			std::unique_lock<std::shared_mutex> lock(mutex_);
			data_iter_->SeekToFirst(); 
		}
    void SeekToLast() override { 
			std::unique_lock<std::shared_mutex> lock(mutex_);
			data_iter_->SeekToLast(); 
		}
    void Next() override { 
			std::unique_lock<std::shared_mutex> lock(mutex_);
			data_iter_->Next(); 
		}
    void Prev() override { 
			std::unique_lock<std::shared_mutex> lock(mutex_);
			data_iter_->Prev(); 
		}
    Status status() const override {
			return data_iter_->status(); 
		}

		Slice key() const override {
      Slice data_key = data_iter_->key();
      memcpy(key_buf, data_key.data(), data_key.size());
      EncodeFixed64(key_buf + data_key.size(), file_number_);
      return Slice(key_buf, data_key.size() + 8);
    } 
		Slice value () const override { 
			return data_iter_->value(); 
		}

		bool NextAndGetResult(IterateResult* result) {
			std::unique_lock<std::shared_mutex> lock(mutex_);
			bool ret = data_iter_->NextAndGetResult(result);
			lock.unlock();
			if (ret) {
				Slice data_key = result->key;
				memcpy(key_buf, data_key.data(), data_key.size());
				EncodeFixed64(key_buf + data_key.size(), file_number_);
				result->key = Slice(key_buf, data_key.size() + 8);
			}
			return ret;
		}
		bool PrepareValue() {
			std::shared_lock<std::shared_mutex> lock(mutex_);
			return data_iter_->PrepareValue();
		}
		bool MayBeOutOfLowerBound() {
			std::shared_lock<std::shared_mutex> lock(mutex_);
			return data_iter_->MayBeOutOfLowerBound();
		}
		IterBoundCheck UpperBoundCheckResult() {
			std::shared_lock<std::shared_mutex> lock(mutex_);
			return data_iter_->UpperBoundCheckResult();
		}
		bool IsKeyPinned() const {
			return data_iter_->IsKeyPinned();
		}
		bool IsValuePinned() const {
			return data_iter_->IsValuePinned();
		}


	private:
    const uint64_t file_number_;
    InternalIterator* data_iter_;
    mutable char key_buf[256];  //need to be larger when key is greater than 248 bytes
		std::shared_mutex mutex_;
};

InternalIterator* NewIteratorWithFileNumber(const uint64_t file_number, InternalIterator* data_iter) {
  return new InternalIteratorWithFileNumber(file_number, data_iter);
}

}