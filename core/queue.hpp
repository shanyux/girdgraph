/*
Copyright (c) 2014-2015 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef QUEUE_H
#define QUEUE_H

#include <queue>
#include <mutex>
#include <condition_variable>

template <typename T>
class Queue {
	const size_t capacity;
	std::queue<T> queue;
	std::mutex mutex;
	std::condition_variable cond_full;
	std::condition_variable cond_empty;
public:
	Queue(const size_t capacity) : capacity(capacity) { }
	void push(const T & item) {
		std::unique_lock<std::mutex> lock(mutex);
		//满了就阻塞，等到pop了再继续执行
		cond_full.wait(lock, [&]{ return !is_full(); });//wait阻塞自己，等待唤醒,只有当 pred 条件为 false 时调用 wait() 才会阻塞当前线程
		queue.push(item);
		lock.unlock();
		cond_empty.notify_one();//notify_one 唤醒一个等待在这个条件变量上的线程
	}
	T pop() {
		std::unique_lock<std::mutex> lock(mutex);
		//没了就阻塞，等到push了再继续执行
		cond_empty.wait(lock, [&]{ return !is_empty(); });
		auto item = queue.front();
		queue.pop();
		lock.unlock();
		cond_full.notify_one();
		return item;
	}
	bool is_full() {
		return queue.size()==capacity;
	}
	bool is_empty() {
		return queue.empty();
	}
};

#endif
