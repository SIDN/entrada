/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].
 *
 */	
package nl.sidn.dnslib.logic.openssl.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import nl.sidn.dnslib.logic.openssl.wrapper.LibCryptoLibrary;
import nl.sidn.dnslib.logic.openssl.wrapper.LibCryptoLibrary.CRYPTO_set_locking_callback_func_callback;

import org.apache.log4j.Logger;
import org.bridj.Pointer;

/**
 * Callback for the openssl library. openssl will call the callback whenever
 * it needs to lock a threads for some time.
 *
 */
public class LockingCallback extends CRYPTO_set_locking_callback_func_callback {
	
	private static final Logger LOGGER = Logger.getLogger(LockingCallback.class);

	private List<Semaphore> locks = new ArrayList<>();
	
	public LockingCallback(){
		int lockCount = LibCryptoLibrary.CRYPTO_num_locks();
		for (int i = 0; i < lockCount; i++) {
			locks.add(new Semaphore(1));
		}
		LOGGER.info("Initialized OpenSSL callback with " + lockCount + " locks");
	}
	
	@Override
	public void apply(int mode, int locknumber, Pointer<Byte> file, int line) {
		if (mode == LibCryptoLibrary.CRYPTO_LOCK){
			try {
				locks.get(locknumber).acquire();
			} catch (InterruptedException e) {
				throw new RuntimeException("Interupted while acquiring lock", e);
			}
		} else{
			locks.get(locknumber).release();
		}
	}

}
