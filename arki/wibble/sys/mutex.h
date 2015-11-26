#ifndef WIBBLE_SYS_MUTEX_H
#define WIBBLE_SYS_MUTEX_H

/*
 * Encapsulated pthread mutex and condition
 *
 * Copyright (C) 2003--2006  Enrico Zini <enrico@debian.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/wibble/exception.h>
#ifdef POSIX
#include <pthread.h>
#endif

#include <errno.h>

namespace wibble {
namespace sys {

/**
 * pthread mutex wrapper; WARNING: the class allows copying and assignment,
 * but this is not always safe. You should never copy a locked mutex. It is
 * however safe to copy when there is no chance of any of the running threads
 * using the mutex.
 */
class Mutex
{
protected:
#ifdef POSIX
	pthread_mutex_t mutex;
#endif

public:
  Mutex(bool recursive = false)
	{
    int res = 0;
#ifdef POSIX
            pthread_mutexattr_t attr;
            pthread_mutexattr_init( &attr );
            if ( recursive ) {
#if (__APPLE__ || __xlC__)
                pthread_mutexattr_settype( &attr, PTHREAD_MUTEX_RECURSIVE );
#else
                pthread_mutexattr_settype( &attr, PTHREAD_MUTEX_RECURSIVE_NP );
#endif
            } else {
#ifndef NDEBUG
                pthread_mutexattr_settype( &attr, PTHREAD_MUTEX_ERRORCHECK_NP );
#endif
            }
    res = pthread_mutex_init(&mutex, &attr);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "creating pthread mutex");
	}

  Mutex( const Mutex & )
	{
    int res = 0;
#ifdef POSIX
            pthread_mutexattr_t attr;
            pthread_mutexattr_init( &attr );
            res = pthread_mutex_init(&mutex, &attr);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "creating pthread mutex");
	}

	~Mutex()
	{
		int res = 0;
#ifdef POSIX
		res = pthread_mutex_destroy(&mutex);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "destroying pthread mutex");
	}

        bool trylock()
        {
    		int res = 0;
#ifdef POSIX
    		res = pthread_mutex_trylock(&mutex);
    		if ( res == EBUSY )
    			return false;
    		if ( res == 0 )
    		  return true;
#endif

    		throw wibble::exception::System(res, "(try)locking pthread mutex");
        }

	/// Lock the mutex
	/// Normally it's better to use MutexLock
	void lock()
	{
		int res = 0;
#ifdef POSIX
		res = pthread_mutex_lock(&mutex);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "locking pthread mutex");
	}

	/// Unlock the mutex
	/// Normally it's better to use MutexLock
	void unlock()
	{
		int res = 0;
#ifdef POSIX
		res = pthread_mutex_unlock(&mutex);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "unlocking pthread mutex");
	}

	/// Reinitialize the mutex
	void reinit()
	{
#ifdef POSIX
		if (int res = pthread_mutex_init(&mutex, 0))
			throw wibble::exception::System(res, "reinitialising pthread mutex");
#endif
	}

	friend class Condition;
};

/**
 * Acquire a mutex lock, RAII-style
 */
template< typename Mutex >
class MutexLockT
{
private:
	// Disallow copy
	MutexLockT(const MutexLockT&);
	MutexLockT& operator=(const MutexLockT&);

public:
	Mutex& mutex;
        bool locked;
        bool yield;

        MutexLockT(Mutex& m) : mutex(m), locked( false ), yield( false ) {
            mutex.lock();
            locked = true;
        }

	~MutexLockT() {
            if ( locked ) {
                mutex.unlock();
                checkYield();
            }
        }

        void drop() {
            mutex.unlock();
            locked = false;
            checkYield();
        }
        void reclaim() { mutex.lock(); locked = true; }
        void setYield( bool y ) {
            yield = y;
        }

        void checkYield() {
            if ( yield )
                sched_yield();
        }

	friend class Condition;
};

typedef MutexLockT< Mutex > MutexLock;

/*
 * pthread condition wrapper.
 *
 * It works in association with a MutexLock.
 *
 * WARNING: the class allows copying and assignment; see Mutex: similar caveats
 * apply. Do not copy or assign a Condition that may be in use.
 */
class Condition
{
protected:
#ifdef POSIX
  pthread_cond_t cond;
#endif

public:
	Condition()
	{
		int res = 0;
#ifdef POSIX
		res = pthread_cond_init(&cond, 0);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "creating pthread condition");
	}

  Condition( const Condition & )
	{
		int res = 0;
#ifdef POSIX
		res = pthread_cond_init(&cond, 0);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "creating pthread condition");
	}

	~Condition()
	{
		int res = 0;
#ifdef POSIX
		res = pthread_cond_destroy(&cond);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "destroying pthread condition");
	}

	/// Wake up one process waiting on the condition
	void signal()
	{
		int res = 0;
#ifdef POSIX
		res = pthread_cond_signal(&cond);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "signaling on a pthread condition");
	}

	/// Wake up all processes waiting on the condition
	void broadcast()
	{
    int res = 0;
#ifdef POSIX
		res = pthread_cond_broadcast(&cond);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "broadcasting on a pthread condition");
	}

	/**
	 * Wait on the condition, locking with l.  l is unlocked before waiting and
	 * locked again before returning.
	 */
    void wait(MutexLock& l)
	{
		int res = 0;
#ifdef POSIX
		res = pthread_cond_wait(&cond, &l.mutex.mutex);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "waiting on a pthread condition");
	}

	void wait(Mutex& l)
	{
		int res = 0;
#ifdef POSIX
		res = pthread_cond_wait(&cond, &l.mutex);
#endif

		if (res != 0)
			throw wibble::exception::System(res, "waiting on a pthread condition");
	}

#ifdef POSIX
	/**
	 * Wait on the condition, locking with l.  l is unlocked before waiting and
	 * locked again before returning.  If the time abstime is reached before
	 * the condition is signaled, then l is locked and the function returns
	 * false.
	 *
	 * @returns
	 *   true if the wait succeeded, or false if the timeout was reached before
	 *   the condition is signaled.
	 */
	bool wait(MutexLock& l, const struct timespec& abstime);
#endif
};

}
}

// vim:set ts=4 sw=4:
#endif
