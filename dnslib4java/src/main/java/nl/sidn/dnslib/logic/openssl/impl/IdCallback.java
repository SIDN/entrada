package nl.sidn.dnslib.logic.openssl.impl;

import nl.sidn.dnslib.logic.openssl.wrapper.LibCryptoLibrary.CRYPTO_set_id_callback_func_callback;

import org.bridj.ann.CLong;

public class IdCallback extends CRYPTO_set_id_callback_func_callback {

	@Override
	@CLong
	public long apply() {
		return Thread.currentThread().getId();
	}

}
