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

#ifndef TIME_H
#define TIME_H

#include <sys/time.h>

inline double get_time() {
	struct timeval tv;// struct timeval {
                    //  time_t       tv_sec;     /* seconds */
                    //  suseconds_t   tv_usec; /* microseconds */
                    //   };
//其中对tv_usec的说明为时间的毫秒部分。 而在实际中，该函数以及Linux内核返回的timeval
//类型的时间值，tv_usec代表的是微秒精度（10的－6次方秒）。

	gettimeofday(&tv, NULL);//int gettimeofday(struct  timeval*tv,struct  timezone *tz )
  //这个函数会把时间包装为一个结构体返回。包括秒，微妙，时区等信息. 
	return tv.tv_sec + (tv.tv_usec / 1e6);
}

#endif
