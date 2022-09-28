package oap.dynamodb.serializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JsonSerializationHelper {
    public static final String CHARACTER_ENCODING = "UTF-8";

    /**
     * base64 encode a byte array using org.apache.commons.codec.binary.Base64
     *
     * @param bytes bytes to encode
     * @return base64 encoded representation of the provided byte array
     */
    public static String base64EncodeByteArray( byte[] bytes ) {
        try {
            byte[] encodeBase64 = Base64.encodeBase64( bytes );
            return new String( encodeBase64, CHARACTER_ENCODING );
        } catch ( Exception e ) {
            throw new RuntimeException( "Exception while encoding bytes: " + Arrays.toString( bytes ) );
        }
    }

    /**
     * base64 decode a base64String using org.apache.commons.codec.binary.Base64
     *
     * @param base64String string to base64 decode
     * @return byte array representing the decoded base64 string
     */
    public static byte[] base64DecodeString( String base64String ) {
        try {
            return Base64.decodeBase64( base64String.getBytes( CHARACTER_ENCODING ) );
        } catch ( Exception e ) {
            throw new RuntimeException( "Exception while decoding " + base64String );
        }
    }

    /**
     * Converts a base64 encoded key into a ByteBuffer
     *
     * @param base64EncodedKey base64 encoded key to be converted
     * @return {@link ByteBuffer} representation of the provided base64 encoded key string
     */
    public static ByteBuffer base64StringToByteBuffer( String base64EncodedKey ) {
        return ByteBuffer.wrap( base64DecodeString( base64EncodedKey ) );
    }

    /**
     * Converts a given list of base64EncodedKeys to a List of ByteBuffers
     *
     * @param base64EncodedKeys base64 encoded key(s) to be converted
     * @return List of {@link ByteBuffer}s representing the provided base64EncodedKeys
     */
    public static List<ByteBuffer> base64StringToByteBuffer( String... base64EncodedKeys ) {
        List<ByteBuffer> byteBuffers = new ArrayList<>( base64EncodedKeys.length );
        for ( String base64EncodedKey : base64EncodedKeys ) {
            byteBuffers.add( base64StringToByteBuffer( base64EncodedKey ) );
        }
        return byteBuffers;
    }

    /**
     * Since ByteBuffer does not have a no-arg constructor we hand serialize/deserialize them.
     */
    private static class ByteBufferSerializer extends JsonSerializer<ByteBuffer> {
        @Override
        public void serialize( ByteBuffer byteBuffer, JsonGenerator jsonGenerator, SerializerProvider serializerProvider ) throws IOException {
            String base64String = base64EncodeByteArray( byteBuffer.array() );
            jsonGenerator.writeString( base64String );
        }
    }

    /**
     * Since ByteBuffer does not have a no-arg constructor we hand serialize/deserialize them.
     */
    private static class ByteBufferDeserializer extends JsonDeserializer<ByteBuffer> {
        @Override
        public ByteBuffer deserialize( JsonParser jsonParser, DeserializationContext deserializationContext ) throws IOException, JacksonException {
            String base64String = jsonParser.getText();
            return base64StringToByteBuffer( base64String );
        }
    }
}
