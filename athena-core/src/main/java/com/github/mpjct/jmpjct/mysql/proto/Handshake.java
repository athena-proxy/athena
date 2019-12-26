package com.github.mpjct.jmpjct.mysql.proto;

import com.github.mpjct.jmpjct.util.PacketUtil;
import com.github.mpjct.jmpjct.util.RandomUtil;
import me.ele.jarch.athena.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class Handshake extends Packet {

    public static final Logger logger = LoggerFactory.getLogger(Handshake.class);
    public static final IdGenerator IDGENERATOR = new IdGenerator();

    public long protocolVersion = 0x0a;
    public String serverVersion = "";

    public long connectionId = 0;
    public byte[] challenge1;// 8 byte
    public long capabilityFlags = Flags.CLIENT_PROTOCOL_41;
    public long characterSet = 0;
    public long statusFlags = 0;
    public byte[] challenge2;// 13 byte 0 terminated
    public long authPluginDataLength = 0;
    public String authPluginName = "";

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

    public void setStatusFlag(long flag) {
        this.statusFlags |= flag;
    }

    public void removeStatusFlag(long flag) {
        this.statusFlags &= ~flag;
    }

    public void toggleStatusFlag(long flag) {
        this.statusFlags ^= flag;
    }

    public boolean hasStatusFlag(long flag) {
        return ((this.statusFlags & flag) == flag);
    }

    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();

        payload.add(Proto.build_fixed_int(1, this.protocolVersion));
        payload.add(Proto.build_null_str(this.serverVersion));
        payload.add(Proto.build_fixed_int(4, this.connectionId));
        payload.add(this.challenge1);
        payload.add(Proto.build_filler(1));
        payload.add(Proto.build_fixed_int(2, this.capabilityFlags & 0xffff));
        payload.add(Proto.build_fixed_int(1, this.characterSet));
        payload.add(Proto.build_fixed_int(2, this.statusFlags));
        payload.add(Proto.build_fixed_int(2, this.capabilityFlags >> 16 & 0xffff));

        if (this.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH)) {
            payload.add(Proto.build_fixed_int(1, this.authPluginDataLength));
        } else {
            payload.add(Proto.build_filler(1));
        }

        payload.add(Proto.build_fixed_str(10, ""));

        if (this.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
            payload.add(this.challenge2);
        }

        if (this.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH)) {
            payload.add(Proto.build_null_str(this.authPluginName));
        }

        return payload;
    }

    public static Handshake loadFromPacket(byte[] packet) {
        Handshake obj = new Handshake();
        Proto proto = new Proto(packet, 3);

        obj.sequenceId = proto.get_fixed_int(1);
        obj.protocolVersion = proto.get_fixed_int(1);
        obj.serverVersion = proto.get_null_str();
        obj.connectionId = proto.get_fixed_int(4);
        obj.challenge1 = proto.get_fixed_bytes(8);
        proto.get_filler(1);
        obj.capabilityFlags = proto.get_fixed_int(2);

        if (proto.has_remaining_data()) {
            obj.characterSet = proto.get_fixed_int(1);
            obj.statusFlags = proto.get_fixed_int(2);
            obj.setCapabilityFlag(proto.get_fixed_int(2) << 16);

            if (obj.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH)) {
                obj.authPluginDataLength = proto.get_fixed_int(1);
            } else {
                proto.get_filler(1);
            }

            proto.get_filler(10);

            if (obj.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
                obj.challenge2 =
                    proto.get_fixed_bytes((int) Math.max(13, obj.authPluginDataLength - 8));
            }

            if (obj.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH)) {
                obj.authPluginName = proto.get_null_str();
            }
        }

        return obj;
    }

    public void pack(long connectionId) {
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        byte[] rand2with0 = new byte[rand2.length + 1];
        System.arraycopy(rand2, 0, rand2with0, 0, rand2.length);
        rand2with0[rand2.length] = 0;

        this.serverVersion = Constants.SERVER_VERSION;
        this.connectionId = connectionId;
        this.challenge1 = rand1;
        this.capabilityFlags = PacketUtil.getServerCapabilities();
        this.characterSet = (byte) (33 & 0xff);// utf8
        this.statusFlags |= Flags.SERVER_STATUS_AUTOCOMMIT; // SERVER_STATUS_AUTOCOMMIT = 1
        this.challenge2 = rand2with0;
        this.authPluginDataLength = 21;
        this.authPluginName = "mysql_native_password";

    }

    /**
     * Generate a increasing ID for each connection.
     * A client usually connects to a couple of DAS servers through a load balancer. In order to avoid a client getting
     * identical connectionID from different DAL servers, assign a random starting value to each DAL process. This is
     * a try-best method.
     */
    public static class IdGenerator {
        private static final Random RANDOM = new Random();
        private static final long START_VALUE = RANDOM.nextInt(1_000_000_000);
        private AtomicLong id = new AtomicLong(START_VALUE);

        public long getId() {
            long result = id.incrementAndGet();
            if (result <= Integer.MAX_VALUE)
                return result;

            synchronized (IdGenerator.class) {
                if (id.get() >= Integer.MAX_VALUE) {
                    id.set(START_VALUE);
                }
                return getId();

            }
        }
    }
}
