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
package nl.sidnlabs.dnslib.types;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Resource Record (RR) TYPEs
 * reference : http://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-4

	A	1	a host address	[RFC1035]
	NS	2	an authoritative name server	[RFC1035]
	MD	3	a mail destination (OBSOLETE - use MX)	[RFC1035]
	MF	4	a mail forwarder (OBSOLETE - use MX)	[RFC1035]
	CNAME	5	the canonical name for an alias	[RFC1035]
	SOA	6	marks the start of a zone of authority	[RFC1035]
	MB	7	a mailbox domain name (EXPERIMENTAL)	[RFC1035]
	MG	8	a mail group member (EXPERIMENTAL)	[RFC1035]
	MR	9	a mail rename domain name (EXPERIMENTAL)	[RFC1035]
	NULL	10	a null RR (EXPERIMENTAL)	[RFC1035]
	WKS	11	a well known service description	[RFC1035]
	PTR	12	a domain name pointer	[RFC1035]
	HINFO	13	host information	[RFC1035]
	MINFO	14	mailbox or mail list information	[RFC1035]
	MX	15	mail exchange	[RFC1035]
	TXT	16	text strings	[RFC1035]
	RP	17	for Responsible Person	[RFC1183]
	AFSDB	18	for AFS Data Base location	[RFC1183][RFC5864]
	X25	19	for X.25 PSDN address	[RFC1183]
	ISDN	20	for ISDN address	[RFC1183]
	RT	21	for Route Through	[RFC1183]
	NSAP	22	for NSAP address, NSAP style A record	[RFC1706]
	NSAP-PTR	23	for domain name pointer, NSAP style	[RFC1348][RFC1637][RFC1706]
	SIG	24	for security signature	[RFC4034][RFC3755][RFC2535][RFC2536][RFC2537][RFC2931][RFC3110][RFC3008]
	KEY	25	for security key	[RFC4034][RFC3755][RFC2535][RFC2536][RFC2537][RFC2539][RFC3008][RFC3110]
	PX	26	X.400 mail mapping information	[RFC2163]
	GPOS	27	Geographical Position	[RFC1712]
	AAAA	28	IP6 Address	[RFC3596]
	LOC	29	Location Information	[RFC1876]
	NXT	30	Next Domain (OBSOLETE)	[RFC3755][RFC2535]
	EID	31	Endpoint Identifier	[Michael_Patton][http://ana-3.lcs.mit.edu/~jnc/nimrod/dns.txt]		1995-06
	NIMLOC	32	Nimrod Locator	[1][Michael_Patton][http://ana-3.lcs.mit.edu/~jnc/nimrod/dns.txt]		1995-06
	SRV	33	Server Selection	[1][RFC2782]
	ATMA	34	ATM Address	[ ATM Forum Technical Committee, "ATM Name System, V2.0", Doc ID: AF-DANS-0152.000, July 2000. Available from and held in escrow by IANA.]
	NAPTR	35	Naming Authority Pointer	[RFC2915][RFC2168][RFC3403]
	KX	36	Key Exchanger	[RFC2230]
	CERT	37	CERT	[RFC4398]
	A6	38	A6 (OBSOLETE - use AAAA)	[RFC3226][RFC2874][RFC6563]
	DNAME	39	DNAME	[RFC6672]
	SINK	40	SINK	[Donald_E_Eastlake][http://tools.ietf.org/html/draft-eastlake-kitchen-sink]		1997-11
	OPT	41	OPT	[RFC6891][RFC3225]
	APL	42	APL	[RFC3123]
	DS	43	Delegation Signer	[RFC4034][RFC3658]
	SSHFP	44	SSH Key Fingerprint	[RFC4255]
	IPSECKEY	45	IPSECKEY	[RFC4025]
	RRSIG	46	RRSIG	[RFC4034][RFC3755]
	NSEC	47	NSEC	[RFC4034][RFC3755]
	DNSKEY	48	DNSKEY	[RFC4034][RFC3755]
	DHCID	49	DHCID	[RFC4701]
	NSEC3	50	NSEC3	[RFC5155]
	NSEC3PARAM	51	NSEC3PARAM	[RFC5155]
	TLSA	52	TLSA	[RFC6698]
	SMIMEA	53	S/MIME cert association	[RFC8162]	SMIMEA/smimea-completed-template	2015-12-01
	Unassigned	54
	HIP	55	Host Identity Protocol	[RFC8005]
	NINFO	56	NINFO	[Jim_Reid]	NINFO/ninfo-completed-template	2008-01-21
	RKEY	57	RKEY	[Jim_Reid]	RKEY/rkey-completed-template	2008-01-21
	TALINK	58	Trust Anchor LINK	[Wouter_Wijngaards]	TALINK/talink-completed-template	2010-02-17
	CDS	59	Child DS	[RFC7344]	CDS/cds-completed-template	2011-06-06
	CDNSKEY	60	DNSKEY(s) the Child wants reflected in DS	[RFC7344]		2014-06-16
	OPENPGPKEY	61	OpenPGP Key	[RFC7929]	OPENPGPKEY/openpgpkey-completed-template	2014-08-12
	CSYNC	62	Child-To-Parent Synchronization	[RFC7477]		2015-01-27
	Unassigned	63-98
	SPF	99		[RFC7208]
	UINFO	100		[IANA-Reserved]
	UID	101		[IANA-Reserved]
	GID	102		[IANA-Reserved]
	UNSPEC	103		[IANA-Reserved]
	NID	104		[RFC6742]	ILNP/nid-completed-template
	L32	105		[RFC6742]	ILNP/l32-completed-template
	L64	106		[RFC6742]	ILNP/l64-completed-template
	LP	107		[RFC6742]	ILNP/lp-completed-template
	EUI48	108	an EUI-48 address	[RFC7043]	EUI48/eui48-completed-template	2013-03-27
	EUI64	109	an EUI-64 address	[RFC7043]	EUI64/eui64-completed-template	2013-03-27
	Unassigned	110-248
	TKEY	249	Transaction Key	[RFC2930]
	TSIG	250	Transaction Signature	[RFC2845]
	IXFR	251	incremental transfer	[RFC1995]
	AXFR	252	transfer of an entire zone	[RFC1035][RFC5936]
	MAILB	253	mailbox-related RRs (MB, MG or MR)	[RFC1035]
	MAILA	254	mail agent RRs (OBSOLETE - see MX)	[RFC1035]
	*	255	A request for all records the server/cache has available	[RFC1035][RFC6895]
	URI	256	URI	[RFC7553]	URI/uri-completed-template	2011-02-22
	CAA	257	Certification Authority Restriction	[RFC6844]	CAA/caa-completed-template	2011-04-07
	AVC	258	Application Visibility and Control	[Wolfgang_Riedel]	AVC/avc-completed-template	2016-02-26
	DOA	259	Digital Object Architecture	[draft-durand-doa-over-dns]	DOA/doa-completed-template	2017-08-30
	Unassigned	260-32767
	TA	32768	DNSSEC Trust Authorities	[Sam_Weiler][http://cameo.library.cmu.edu/][ Deploying DNSSEC Without a Signed Root. Technical Report 1999-19, Information Networking Institute, Carnegie Mellon University, April 2004.]		2005-12-13
	DLV	32769	DNSSEC Lookaside Validation	[RFC4431]
	Unassigned	32770-65279
	Private use	65280-65534
	Reserved	65535
 *
 */
public enum ResourceRecordType {
	
	A(1),
	NS(2),
	MD(3),
	MF(4),
	CNAME(5),
	SOA(6),
	MB(7),
	MG(8),
	MR(9),
	NULL(10),
	WKS(11),
	PTR(12),
	HINFO(13),
	MINFO(14),
	MX(15),
	TXT(16),
	RP(17),
	AFSDB(18),
	X25(19),
	ISDN(20),
	RT(21),
	NSAP(22),
	NSAP_PTR(23),
	SIG(24),
	KEY(25),
	PX(26),
	GPOS(27),
	AAAA(28),
	LOC(29),
	NXT(30),
	EID(31),
	NIMLOC(32),
	SRV(33),
	ATMA(34),
	NAPTR(35),
	KX(36),
	CERT(37),
	A6(38),
	DNAME(39),
	SINK(40),
	OPT(41),
	APL(42),
	DS(43),		
	SSHFP(44),
	IPSECKEY(45),
	RRSIG(46),
	NSEC(47),
	DNSKEY(48),
	DHCID(49),	
	NSEC3(50),
	NSEC3PARAM(51),
	TLSA(52),
	SMIMEA(53),
	//Unassigned 54
	UNASSIGNED_54(54),
	HIP(55),
	NINFO(56),
	RKEY(57),
	TALINK(58),
	CDS(59),
	CDNSKEY(60),
	OPENPGPKEY(61),
	CSYNC(62),
	//Unassigned	63-98
	SPF(99),
	UINFO(100),
	UID(101),
	GID(102),
	UNSPEC(103),
	NID(104),
	L32(105),
	L64(106),
	LP(107),
	EUI48(108),
	EUI64(109),
	//Unassigned	110-248	
	TKEY(249),
	TSIG(250),
	IXFR(251),
	AXFR(252),
	MAILB(253),
	MAILA(254),
	ANY(255),
	URI(256),
	CAA(257),
	AVC(258),
	DOA(259),
	//Unassigned	260-32767
	TA(32768),
	DLV(32769),
	//Unassigned	32770-65279				
	//Private use	65280-65534		
	RESERVED(65535),
	UNASSIGNED(-1),
	PRIVATE(-2),
	UNKNOWN(-3);
	
	
	private int value;
	
	private static Map<String, ResourceRecordType> types = new HashMap<>();
	private static Map<Integer, ResourceRecordType> typesToInt = new HashMap<>();
	
	static{
		ResourceRecordType[] values = values();
		for (ResourceRecordType type : values) {
			types.put(type.name(), type);
			typesToInt.put(new Integer(type.getValue()), type);
		}
	}
	
	private ResourceRecordType(int value){
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	public static ResourceRecordType fromString(String name){
		return types.get(StringUtils.upperCase(name));
	}
	
	public static ResourceRecordType fromValue(int value){
		ResourceRecordType type = typesToInt.get(new Integer(value));
		if(type == null){
			//Unassigned	54
			//Unassigned	63-98
			//Unassigned	110-248
			//Unassigned	260-32767
			//Unassigned	32770-65279
			if( (value == 54) ||
				(value >= 63 &&  value <= 98) ||
				(value >= 110 &&  value <= 248) ||
				(value >= 260 &&  value <= 32767) ||
				(value >= 32770 &&  value <= 65279)
			  ){
				return UNASSIGNED;
			}else if(value >= 65280 &&  value <= 65534){
				//Private use	65280-65534
				return PRIVATE;
			}
			//no match
			return UNKNOWN;
		}
		
		return type;
	}
}
