

// design a message receiver and timer
// requirements:
// 	- receiver and timer should run in independent threads
// 	- receiver should continue receiving messages while the timer is running

/*
 * 
 * 
 *
 *
 */




class TimedReceiver {
  public:
    void start() {
      start_timer();
      start_receiver();
    }

    void start_timer() {
      while (true) {
	auto st = timer_cv.wait(timer_mutex, timeout_duration, []() {
	        return false;
	    });

	if (st == std::cv_status::timeout) {
	  change_state();
	}
      }
    }

    // receiver continues to receive messages while the timer is running
    // receiver might receive multiple messages in parallel
    // receiver shouldn't block while processsing a message
    //
    void start_receiver() {
      ssize_t n = ::recv(sockfd_, buf, BUFBYTES); 

      message_t msg{buf, n};
      // process message in pool
      pool.add(msg);
    }

  private:
    std::chrono::milliseconds timeout_duration{};

    std::mutex recv_mutex;
    std::mutex timer_mutex;

    std::condition_variable timer_cv;
    std::condition_variable recv_cv;

    std::jthread receiver_thr;
    std::jthread timer_thr{};

    thread_pool pool;
};
