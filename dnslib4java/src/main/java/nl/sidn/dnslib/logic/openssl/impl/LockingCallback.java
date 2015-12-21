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
