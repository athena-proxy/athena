package com.github.mpjct.jmpjct.mysql.proto;

import com.github.mpjct.jmpjct.util.CharsetUtil;
import com.github.mpjct.jmpjct.util.PacketUtil;
import com.github.mpjct.jmpjct.util.SecurityUtil;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Constants;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class HandshakeResponse extends Packet {
    private Handshake handshake;

    public long capabilityFlags = Flags.CLIENT_PROTOCOL_41;
    public long maxPacketSize = 0;
    public long characterSet = 0;
    public String username = "";
    public int authResponseLen = 0;
    public byte[] authResponse;
    public String schema = "";
    public String pluginName = "";
    public long clientAttributesLen = 0;
    public String clientAttributes = "";
    private DBConnectionInfo dbConnInfo;

    HandshakeResponse() {

    }

    public HandshakeResponse(DBConnectionInfo info, Handshake handshake) {
        this.handshake = handshake;
        this.dbConnInfo = info;
    }

    public void setCapabilityFlag(long flag) {
        this.capabilityFlags |= flag;
    }

    public void removeCapabilityFlag(long flag) {
        this.capabilityFlags &= ~flag;
    }

    public void toggleCapabilityFlag(long flag) {
        this.capabilityFlags ^= flag;
    }

    public boolean hasCapabilityFlag(long flag) {
        return ((this.capabilityFlags & flag) == flag);
    }

    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();

        if ((this.capabilityFlags & Flags.CLIENT_PROTOCOL_41) != 0) {
            payload.add(Proto.build_fixed_int(4, this.capabilityFlags));
            payload.add(Proto.build_fixed_int(4, this.maxPacketSize));
            payload.add(Proto.build_fixed_int(1, this.characterSet));
            payload.add(Proto.build_filler(23));
            payload.add(Proto.build_null_str(this.username));
            if (this.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
                payload.add(Proto.build_lenenc_int(this.authResponseLen));
                payload.add(this.authResponse);
            } else {
                if (this.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
                    payload.add(Proto.build_fixed_int(1, this.authResponseLen));
                    payload.add(this.authResponse);
                } else
                    payload.add(this.authResponse);
            }

            if (this.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB))
                payload.add(Proto.build_null_str(this.schema));

            if (this.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH))
                payload.add(Proto.build_null_str(this.pluginName));

            if (this.hasCapabilityFlag(Flags.CLIENT_CONNECT_ATTRS)) {
                payload.add(Proto.build_lenenc_int(this.clientAttributesLen));
                payload.add(Proto.build_eop_str(this.clientAttributes));
            }
        } else {
            payload.add(Proto.build_fixed_int(2, this.capabilityFlags));
            payload.add(Proto.build_fixed_int(3, this.maxPacketSize));
            payload.add(Proto.build_null_str(this.username));

            if (this.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB)) {
                payload.add(this.authResponse);
                payload.add(Proto.build_null_str(this.schema));
            } else
                payload.add(this.authResponse);

        }

        return payload;
    }

    public static HandshakeResponse loadFromPacket(byte[] packet) {
        HandshakeResponse obj = new HandshakeResponse();
        Proto proto = new Proto(packet, 3);

        obj.sequenceId = proto.get_fixed_int(1);
        obj.capabilityFlags = proto.get_fixed_int(2);
        proto.offset -= 2;

        if (obj.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
            obj.capabilityFlags = proto.get_fixed_int(4);
            obj.maxPacketSize = proto.get_fixed_int(4);
            obj.characterSet = proto.get_fixed_int(1);
            proto.get_filler(23);
            obj.username = proto.get_null_str();

            if (obj.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
                obj.authResponseLen = (int) proto.get_lenenc_int();
                obj.authResponse = proto.get_fixed_bytes(obj.authResponseLen);
            } else {
                if (obj.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
                    obj.authResponseLen = (int) proto.get_fixed_int(1);
                    obj.authResponse = proto.get_fixed_bytes(obj.authResponseLen);
                } else {
                    obj.authResponse = proto.get_null_bytes();
                }
            }

            if (obj.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB))
                obj.schema = proto.get_null_str();

            if (obj.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH))
                obj.pluginName = proto.get_null_str();

            if (obj.hasCapabilityFlag(Flags.CLIENT_CONNECT_ATTRS)) {
                obj.clientAttributesLen = proto.get_lenenc_int();
                StringBuilder sb = new StringBuilder((int) obj.clientAttributesLen);
                while (proto.packet.length > proto.offset) {
                    sb.append(proto.get_lenenc_str());
                }
                obj.clientAttributes = sb.toString();
            }
        } else {
            obj.capabilityFlags = proto.get_fixed_int(2);
            obj.maxPacketSize = proto.get_fixed_int(3);
            obj.username = proto.get_null_str();

            if (obj.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB)) {
                obj.authResponse = proto.get_null_bytes();
                obj.schema = proto.get_null_str();
            } else {
                obj.authResponse = proto.get_eop_bytes();
            }
        }

        return obj;
    }

    public void pack() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        this.capabilityFlags = PacketUtil.getClientFlags();
        this.maxPacketSize = Constants.MAX_PACKET_SIZE;
        this.characterSet = handshake.characterSet & 0xff;
        this.schema = dbConnInfo.getDatabase();
        this.username = dbConnInfo.getUser();
        String passwd = dbConnInfo.getPword();
        this.sequenceId = 1;
        String charset = CharsetUtil.getCharset((int) (characterSet & 0xff));
        if (passwd != null && passwd.length() > 0) {
            byte[] password = passwd.getBytes(charset);
            byte[] seed = handshake.challenge1;
            byte[] restOfScramble = handshake.challenge2;
            byte[] authSeed = new byte[seed.length + restOfScramble.length - 1];
            System.arraycopy(seed, 0, authSeed, 0, seed.length);
            System.arraycopy(restOfScramble, 0, authSeed, seed.length, restOfScramble.length - 1);
            byte[] response = SecurityUtil.scramble411(password, authSeed);
            this.authResponseLen = response.length;
            this.authResponse = response;
        }

    }
}
