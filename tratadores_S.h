#ifndef TRATADORES_S_H
#define TRATADORES_S_H

//#pragma once
#include <vector>
#include <functional>
#include <string>
#include "df.h"
#include "threads.h"
using namespace std;

template <typename T>
T accumulate(const vector<T>& vec, T init);

vector<int> filter_block_records(const class DataFrame& df, function<bool(const vector<ElementType>&)> condition, int idx_min, int idx_max);

class DataFrame filter_records_by_idxes(const class DataFrame& df, const vector<int>& idxes);

class DataFrame filter_records(class DataFrame& df, function<bool(const vector<ElementType>&)> condition, int num_threads);

class DataFrame groupby_mean(class DataFrame& df, const string& group_col, const string& value_col, int num_threads);

class DataFrame join_by_key(const class DataFrame& left, const class DataFrame& right, const string& key_col, int num_threads);

namespace multiprocessing
{
    DataFrame filter_records(DataFrame& df, function<bool(const vector<ElementType>&)> condition, int num_threads, ThreadPool& pool);
    DataFrame groupby_mean(DataFrame& df, const string& group_col, const string& target_col, ThreadPool& pool);
    DataFrame join_by_key(const DataFrame& df1, const DataFrame& df2, const string& key_col, ThreadPool& pool);
}

#endif