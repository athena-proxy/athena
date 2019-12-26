package me.ele.jarch.athena.netty;

public class SqlPacketDecoderTest {
    //
    // @Test
    // public void testDecodeOnePacket() throws Exception {
    // assertEquals(Packet.getSize(new byte[] { 0x1, 0x2, 0x0 }), 513);
    // ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer();
    //
    // AbstractSqlPacketDecoder decoder = new SqlClientPacketDecoder( new Scheduler( null ));
    // decoder.setSqlCtx(null);
    //
    // buf.writeBytes(new byte[] { 0x1 });
    //
    // buf.writeBytes(new byte[] { 0x2 });
    // decoder.decode(null, buf, null);
    // Assert.assertEquals(decoder.getPackets().size(), 0);
    //
    // byte[] b = new byte[1 + 1 + 413];// 0x0 + sequenceId + 413 bytes
    // final int sequenceId = 100;
    // b[0] = 0x0;
    // b[1] = sequenceId;
    // buf.writeBytes(b);
    // decoder.decode(null, buf, null);
    // Assert.assertEquals(decoder.getPackets().size(), 0);
    //
    // // write remaining 100 bytes
    // buf.writeBytes(new byte[100]);
    // decoder.decode(null, buf, null);
    //
    // Assert.assertEquals(decoder.getPackets().size(), 1);
    // }
    //
    // @Test
    // public void testDecodeTwoIntermitentPackets() throws Exception {
    // assertEquals(Packet.getSize(new byte[] { 0x1, 0x2, 0x0 }), 513);
    // ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer();
    //
    // AbstractSqlPacketDecoder decoder = new SqlClientPacketDecoder( new Scheduler( null ));
    // decoder.setSqlCtx(null);
    //
    // buf.writeBytes(new byte[] { 0x1 });
    //
    // buf.writeBytes(new byte[] { 0x2 });
    // decoder.decode(null, buf, null);
    //
    // byte[] b = new byte[1 + 1 + 413 + 1];// 0x0 + sequenceId + 413 bytes, an extra byte that should be in the next packet.
    // final int sequenceId = -1;
    // b[0] = 0x0;
    // b[1] = sequenceId;
    //
    // b[1 + 1 + 413] = 0x1; // last index, fill the size byte
    // buf.writeBytes(b);
    // decoder.decode(null, buf, null);
    // Assert.assertEquals(decoder.getPackets().size(), 0);
    //
    // // write remaining 100 bytes
    // buf.writeBytes(new byte[100]);
    // decoder.decode(null, buf, null);
    //
    // Assert.assertEquals(decoder.getPackets().size(), 1);
    //
    // buf.writeBytes(new byte[] { 0x2 });
    // buf.writeBytes(b);
    // buf.writeBytes(new byte[100]);
    // decoder.decode(null, buf, null);
    // Assert.assertEquals(decoder.getPackets().size(), 2);
    //
    // }
    //
    // @Test
    // public void testDecodeTwoCompletePackets() throws Exception {
    // assertEquals(Packet.getSize(new byte[] { 0x1, 0x2, 0x0 }), 513);
    // ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer();
    //
    // // first packet
    // buf.writeBytes(new byte[] { 0x1, 0x2, 0x0, 0x00 });
    // buf.writeBytes(new byte[513]);
    //
    // // second packet
    // buf.writeBytes(new byte[] { 0x1, 0x2, 0x0, 0x00 });
    // buf.writeBytes(new byte[513]);
    //
    // // extra bytes of the third packets
    // buf.writeBytes(new byte[] { 0x2 });
    //
    // AbstractSqlPacketDecoder decoder = new SqlClientPacketDecoder( new Scheduler( null));
    // decoder.setSqlCtx(null);
    // decoder.decode(null, buf, null);
    //
    // Assert.assertEquals(decoder.getPackets().size(), 2);
    // }

}
