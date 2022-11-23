#pragma once

#include "table/internal_iterator.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class InternalIteratorWithFileNumber : public InternalIterator {
	public:
		InternalIteratorWithFileNumber(const uint64_t file_number, InternalIterator* data_iter)
													: file_number_(file_number),
														data_iter_(data_iter) {}
		~InternalIteratorWithFileNumber() { delete data_iter_; } // need delete ???

		bool Valid() const override { return data_iter_->Valid(); }
    void Seek(const Slice& target) override { data_iter_->Seek(target); }
		void SeekForPrev(const Slice& target) { data_iter_->SeekForPrev(target); }
    void SeekToFirst() override { data_iter_->SeekToFirst(); }
    void SeekToLast() override { data_iter_->SeekToLast(); }
    void Next() override { data_iter_->Next(); }
    void Prev() override { data_iter_->Prev(); }
    Status status() const override { return data_iter_->status(); }

		Slice key() const override {
      Slice data_key = data_iter_->key();
      memcpy(key_buf, data_key.data(), data_key.size());
      EncodeFixed64(key_buf + data_key.size(), file_number_);
      return Slice(key_buf, data_key.size() + 8);
    } 
		Slice value () const override { return data_iter_->value(); }

		bool NextAndGetResult(IterateResult* result) {
			return data_iter_->NextAndGetResult(result);
		}
		bool PrepareValue() {return data_iter_->PrepareValue();}
		bool MayBeOutOfLowerBound() {return data_iter_->MayBeOutOfLowerBound();}
		IterBoundCheck UpperBoundCheckResult() {data_iter_->UpperBoundCheckResult();}
		bool IsKeyPinned() const {return data_iter_->IsKeyPinned();}
		bool IsValuePinned() const {return data_iter_->IsValuePinned();}


	private:
    const uint64_t file_number_;
    InternalIterator* data_iter_;
    mutable char key_buf[256];  //need to be larger when key is greater than 248 bytes
};

InternalIterator* NewIteratorWithFileNumber(const uint64_t file_number, InternalIterator* data_iter) {
  return new InternalIteratorWithFileNumber(file_number, data_iter);
}

}