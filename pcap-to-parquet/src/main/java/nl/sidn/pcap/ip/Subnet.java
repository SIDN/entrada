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
package nl.sidn.pcap.ip;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author c3oe.de, based on snippets from Scott Plante, John Kugelmann
 */
public class Subnet
{
    final private int bytesSubnetCount;
    final private BigInteger bigMask;
    final private BigInteger bigSubnetMasked;

    /** For use via format "192.168.0.0/24" or "2001:db8:85a3:880:0:0:0:0/57" */
    public Subnet( final InetAddress subnetAddress, final int bits )
    {
        this.bytesSubnetCount = subnetAddress.getAddress().length; // 4 or 16
        this.bigMask = BigInteger.valueOf( -1 ).shiftLeft( this.bytesSubnetCount*8 - bits ); // mask = -1 << 32 - bits
        this.bigSubnetMasked = new BigInteger( subnetAddress.getAddress() ).and( this.bigMask );
    }

    /** For use via format "192.168.0.0/255.255.255.0" or single address */
    public Subnet( final InetAddress subnetAddress, final InetAddress mask )
    {
        this.bytesSubnetCount = subnetAddress.getAddress().length;
        this.bigMask = null == mask ? BigInteger.valueOf( -1 ) : new BigInteger( mask.getAddress() ); // no mask given case is handled here.
        this.bigSubnetMasked = new BigInteger( subnetAddress.getAddress() ).and( this.bigMask );
    }

    /**
     * Subnet factory method.
     * @param subnetMask format: "192.168.0.0/24" or "192.168.0.0/255.255.255.0"
     *      or single address or "2001:db8:85a3:880:0:0:0:0/57"
     * @return a new instance
     * @throws UnknownHostException thrown if unsupported subnet mask.
     */
    public static Subnet createInstance( final String subnetMask )
            throws UnknownHostException
    {
        final String[] stringArr = subnetMask.split("/");
        if ( 2 > stringArr.length )
            return new Subnet( InetAddress.getByName( stringArr[ 0 ] ), (InetAddress)null );
        else if ( stringArr[ 1 ].contains(".") || stringArr[ 1 ].contains(":") )
            return new Subnet( InetAddress.getByName( stringArr[ 0 ] ), InetAddress.getByName( stringArr[ 1 ] ) );
        else
            return new Subnet( InetAddress.getByName( stringArr[ 0 ] ), Integer.parseInt( stringArr[ 1 ] ) );
    }

    public boolean isInNet( final InetAddress address )
    {
        final byte[] bytesAddress = address.getAddress();
        if ( this.bytesSubnetCount != bytesAddress.length )
            return false;
        final BigInteger bigAddress = new BigInteger( bytesAddress );
        return  bigAddress.and( this.bigMask ).equals( this.bigSubnetMasked );
    }

    @Override
    final public boolean equals( Object obj )
    {
        if ( ! (obj instanceof Subnet) )
            return false;
        final Subnet other = (Subnet)obj;
        return  this.bigSubnetMasked.equals( other.bigSubnetMasked ) &&
                this.bigMask.equals( other.bigMask ) &&
                this.bytesSubnetCount == other.bytesSubnetCount;
    }

    @Override
    final public int hashCode()
    {
        return this.bytesSubnetCount;
    }

    @Override
    public String toString()
    {
        final StringBuilder buf = new StringBuilder();
        bigInteger2IpString( buf, this.bigSubnetMasked, this.bytesSubnetCount );
        buf.append( '/' );
        bigInteger2IpString( buf, this.bigMask, this.bytesSubnetCount );
        return buf.toString();
    }

    static private void bigInteger2IpString( final StringBuilder buf, final BigInteger bigInteger, final int displayBytes )
    {
        final boolean isIPv4 = 4 == displayBytes;
        byte[] bytes = bigInteger.toByteArray();
        int diffLen = displayBytes - bytes.length;
        final byte fillByte = 0 > (int)bytes[ 0 ] ? (byte)0xFF : (byte)0x00;

        int integer;
        for ( int i = 0; i < displayBytes; i++ )
        {
            if ( 0 < i && ! isIPv4 && i % 2 == 0 )
                buf.append( ':' );
            else if ( 0 < i && isIPv4 )
                buf.append( '.' );
            integer = 0xFF & (i < diffLen ? fillByte : bytes[ i - diffLen ]);
            if ( ! isIPv4 && 0x10 > integer )
                buf.append( '0' );
            buf.append( isIPv4 ? integer : Integer.toHexString( integer ) );
        }
    }
}