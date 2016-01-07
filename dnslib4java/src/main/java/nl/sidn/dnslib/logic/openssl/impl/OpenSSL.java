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

import nl.sidn.dnslib.logic.openssl.wrapper.LibCryptoLibrary;
import nl.sidn.dnslib.logic.openssl.wrapper.LibCryptoLibrary.CRYPTO_set_locking_callback_func_callback;

import org.apache.log4j.Logger;
import org.bridj.BridJ;
import org.bridj.Pointer;

/**
 * Utility used to correctly initialze the openssl library threading.
 * If this is not done then the JVM will crash at random moments with double free error.
 * @see http://www.openssl.org/docs/crypto/threads.html
 */
public class OpenSSL {
	
	private static final Logger LOGGER = Logger.getLogger(OpenSSL.class);
	
	private static LibCryptoLibrary.CRYPTO_set_id_callback_func_callback idcb;
	private static CRYPTO_set_locking_callback_func_callback LockingCallback;
	
	public static void init(){
		LOGGER.info("Initialize OpenSSL"); 
		idcb = new IdCallback();
		LibCryptoLibrary.CRYPTO_set_id_callback(Pointer.pointerTo(idcb));
		/* make sure the callbacks do not get garbage collected */
		BridJ.protectFromGC(idcb);
		
		LockingCallback = new LockingCallback();
		LibCryptoLibrary.CRYPTO_set_locking_callback(Pointer.pointerTo(LockingCallback));
		BridJ.protectFromGC(LockingCallback);

	}
	
	public static void cleanup(){
		LOGGER.info("Cleanup OpenSSL");
		BridJ.unprotectFromGC(idcb);
		BridJ.unprotectFromGC(LockingCallback);
	}

}
