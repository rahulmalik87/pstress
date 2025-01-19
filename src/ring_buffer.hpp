#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#include <mutex>
#include <string>
#include <vector>

template <typename T>
class RingBuffer {
public:
  RingBuffer(size_t capacity)
      : capacity_(capacity), buffer_(capacity), head_(0), tail_(0), size_(0) {}

  void push(const T &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (size_ == capacity_) {
      head_ = (head_ + 1) % capacity_;
    } else {
      ++size_;
    }
    buffer_[tail_] = value;
    tail_ = (tail_ + 1) % capacity_;
  }

  std::vector<T> get_all() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<T> result(size_);
    size_t idx = head_;
    for (size_t i = 0; i < size_; ++i) {
      result[i] = buffer_[idx];
      idx = (idx + 1) % capacity_;
    }
    return result;
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    size_ = 0;
    head_ = tail_ = 0;
  }

  void resize(size_t new_capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<T> new_buffer(new_capacity);
    size_t current_size = std::min(size_, new_capacity);
    for (size_t i = 0; i < current_size; ++i) {
      new_buffer[i] = buffer_[(head_ + i) % capacity_];
    }
    buffer_ = std::move(new_buffer);
    capacity_ = new_capacity;
    head_ = 0;
    tail_ = current_size % new_capacity;
    size_ = current_size;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return size_;
  }

private:
  size_t capacity_;
  std::vector<T> buffer_;
  size_t head_;
  size_t tail_;
  size_t size_;
  mutable std::mutex mutex_;
};

#endif // RING_BUFFER_H
