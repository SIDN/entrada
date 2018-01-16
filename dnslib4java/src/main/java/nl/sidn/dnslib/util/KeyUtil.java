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
package nl.sidn.dnslib.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.EllipticCurve;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECFieldFp;
import java.security.spec.ECPoint;

import nl.sidn.dnslib.message.records.dnssec.DNSKEYResourceRecord;
import nl.sidn.dnslib.message.records.dnssec.DSResourceRecord;

public class KeyUtil {
	
	private static char KEY_ZONE_FLAG_MASK = 0x0100; //0000 0001 0000 0000
	private static char KEY_ZONE_SEP_FLAG_MASK = 0x0101; //0000 0001 0000 0001
	
	public static PublicKey createPublicKey(byte[] key, int algorithm) {
		if (algorithm == 13) {
			return createECPublicKey(key);
		} else {
			// Maybe not super clean to let all the other
			// algorithms (!= 13) being handled by the following method,
			// but a start.
			return createRSAPublicKey(key);
		}
	}
	
	public static PublicKey createRSAPublicKey(byte[] key) {
		ByteBuffer b = ByteBuffer.wrap(key);
		
		int exponentLength = b.get() & 0xff;
		if (exponentLength == 0){
			exponentLength = b.getChar();
		}
		try {
			byte [] data = new byte[exponentLength];
			b.get(data);
			BigInteger exponent =  new BigInteger(1, data);
			byte [] modulusData = new byte[b.remaining()];
			b.get(modulusData);
			BigInteger modulus = new BigInteger(1, modulusData);

			KeyFactory factory = KeyFactory.getInstance("RSA");
			return factory.generatePublic(new RSAPublicKeySpec(modulus, exponent));
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
			throw new RuntimeException("Error creating public key", e);
		}
	}
	
	public static PublicKey createECPublicKey(byte[] key) {
		// RFC 5114 Section 2.6
		// ECDSA_P256
		// Algorithm 13
		String p_str = "FFFFFFFF00000001000000000000000000000000FFFFFFFFFFFFFFFFFFFFFFFF";
		String a_str = "FFFFFFFF00000001000000000000000000000000FFFFFFFFFFFFFFFFFFFFFFFC";
		String b_str = "5AC635D8AA3A93E7B3EBBD55769886BC651D06B0CC53B0F63BCE3C3E27D2604B";
		String gx_str = "6B17D1F2E12C4247F8BCE6E563A440F277037D812DEB33A0F4A13945D898C296";
		String gy_str = "4FE342E2FE1A7F9B8EE7EB4A7C0F9E162BCE33576B315ECECBB6406837BF51F5";
		String n_str = "FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551";
		BigInteger p, a, b, gx, gy, n;
		p = new BigInteger(p_str, 16);
		a = new BigInteger(a_str, 16);
		b = new BigInteger(b_str, 16);
		gx = new BigInteger(gx_str, 16);
		gy = new BigInteger(gy_str, 16);
		n = new BigInteger(n_str, 16);
		try {
			ByteBuffer buff = ByteBuffer.wrap(key);
			byte [] x_bytes = new byte[key.length/2];
			byte [] y_bytes = new byte[key.length/2];
			buff.get(x_bytes);
			buff.get(y_bytes);
			BigInteger x = new BigInteger(1, x_bytes);
			BigInteger y = new BigInteger(1, y_bytes);
			ECPoint q = new ECPoint(x, y);
			EllipticCurve curve = new EllipticCurve(new ECFieldFp(p), a, b);
			ECParameterSpec spec = new ECParameterSpec(curve, new ECPoint(gx, gy), n, 1);
			
			KeyFactory factory = KeyFactory.getInstance("EC");
			return factory.generatePublic(new ECPublicKeySpec(q, spec));
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
			throw new RuntimeException("Error creating public key for ECDSA (algorithm 13)", e);
		}
	}
	
	/**
	 * Bereken de keyTag(footprint) van een publieke sleutel.
	 * De keyTag berekent een getal waarmee de publieke sleutel te herkennen is, dit is 
	 * niet per definitie uniek per publieke sleutel.
	 * Zie IETF RFC 4034, Appendix B voor meer informatie.
	 * @see http://www.ietf.org/rfc/rfc4034.txt
	 * 
	 * Dit lijkt op het berekenen van 1 complement checksum (http://nl.wikipedia.org/wiki/One%27s_complement)
	 * De onderstaande implementatie is overgenomen van versisign, zie:
	 * http://svn.verisignlabs.com/jdnssec/dnsjava/trunk/org/xbill/DNS/KEYBase.java
	 * @param key een base64 encoded public key
	 * @param algorimte, de naam van het algoritme waarmee de public key is gemaakt.
	 * @return integer waarde welke de keytag van de public key is
	 */
	public static int createKeyTag(byte[] rdata, int alg) {
		
		int foot = 0;
		int footprint = -1;

		// als de publieke sleuten met RSA/MD5 is gemaakt en gehashed dan
		// geld er een ander algoritme voor bepalen keytag

		if (1 == alg) {  //MD5
			int d1 = rdata[rdata.length - 3] & 0xFF;
			int d2 = rdata[rdata.length - 2] & 0xFF;
			foot = (d1 << 8) + d2;
		} else {
			int i;
			for (i = 0; i < rdata.length - 1; i += 2) {
				int d1 = rdata[i] & 0xFF;
				int d2 = rdata[i + 1] & 0xFF;
				foot += ((d1 << 8) + d2);
			}
			if (i < rdata.length) {
				int d1 = rdata[i] & 0xFF;
				foot += (d1 << 8);
			}
			foot += ((foot >> 16) & 0xFFFF);
		}
		footprint = (foot & 0xFFFF);
		return footprint;
	}
	
	public static boolean isZoneKey(DNSKEYResourceRecord key){
		return (key.getFlags() & KEY_ZONE_FLAG_MASK) == KEY_ZONE_FLAG_MASK;
	}
	
	public static boolean isSepKey(DNSKEYResourceRecord key){
		return (key.getFlags() & KEY_ZONE_SEP_FLAG_MASK) == KEY_ZONE_SEP_FLAG_MASK;
	}
	
	public static boolean isKeyandDSmatch(DNSKEYResourceRecord key, DSResourceRecord ds){
		if(key.getAlgorithm() == ds.getAlgorithm() &&
				key.getKeytag() == ds.getKeytag() &&
				key.getName().equalsIgnoreCase(ds.getName())  ){
			return true;
		}
		
		return false;
	}

}
