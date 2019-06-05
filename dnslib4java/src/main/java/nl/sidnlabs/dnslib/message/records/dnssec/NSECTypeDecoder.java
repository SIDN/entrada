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
package nl.sidnlabs.dnslib.message.records.dnssec;

import java.util.List;
import nl.sidnlabs.dnslib.message.util.NetworkData;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.dnslib.types.TypeMap;

public class NSECTypeDecoder {

	/**
	 * Decode the type bitmap of the NSEC and NSEC3 RR
	 * @param octetAvailable
	 * @param buffer
	 * @param types
	 */
	public void decode(int octetAvailable, NetworkData buffer,
			List<TypeMap> types) {

		if (octetAvailable > 0) {
			/* data left with the type flags */
			int octetsRead = 0;
			// keep reading types until all the rdata has been read
			while (octetsRead < octetAvailable) {
				// read the block # of the types
				short blockCount = buffer.readUnsignedByte();
				// read the number of bytes in the block
				short blockLength = buffer.readUnsignedByte();
				// we have read 2 bytes
				octetsRead = octetsRead + 2;
				/* read the block byte by byte */
				for (int i = 0; i < blockLength; i++) {
					// read the flags one byte at a time
					short flags = buffer.readUnsignedByte();
					// inc the read counter
					octetsRead++;

					int flag = 0;
					if (flags == 0x0) {
						flag = 0;
					} else {
						if ((flags & 0x0080) == 0x0080) { // 0000 0000 1000 0000
							// 1st bit is set
							flag = (i * 8) + 0;
							createRRtype(flag, blockCount, types);
						}
						if ((flags & 0x0040) == 0x0040) { // 0000 0000 0100 0000
							// 2nd bit is set
							flag = (i * 8) + 1;
							createRRtype(flag, blockCount, types);
						}
						if ((flags & 0x0020) == 0x0020) { // 0000 0000 0010 0000
							// 3rd bit is set
							flag = (i * 8) + 2;
							createRRtype(flag, blockCount, types);
						}
						if ((flags & 0x0010) == 0x0010) { // 0000 0000 0001 0000
							// 4th bit is set
							flag = (i * 8) + 3;
							createRRtype(flag, blockCount, types);
						}
						if ((flags & 0x0008) == 0x0008) { // 0000 0000 0000 1000
							// 5th bit is set
							flag = (i * 8) + 4;
							createRRtype(flag, blockCount, types);
						}
						if ((flags & 0x0004) == 0x0004) { // 0000 0000 0000 0100
							// 6th bit is set
							flag = (i * 8) + 5;
							createRRtype(flag, blockCount, types);
						}
						if ((flags & 0x0002) == 0x0002) { // 0000 0000 0000 0010
							// 7th bit is set
							flag = (i * 8) + 6;
							createRRtype(flag, blockCount, types);
						}
						if ((flags & 0x0001) == 0x0001) { // 0000 0000 0000 0001
							// 8th bit is set
							flag = (i * 8) + 7;
							createRRtype(flag, blockCount, types);
						}
					}

				}
			}
		}
	}

	private void createRRtype(int flag, short blockCount, List<TypeMap> types) {
		if (flag > 0) {
			flag = flag + (blockCount * 256);
			ResourceRecordType rrType = ResourceRecordType.fromValue((char) flag);
			if(rrType == null){
				rrType = ResourceRecordType.RESERVED;
			}
			types.add(new TypeMap(rrType, (char)flag));
		}
	}
}
