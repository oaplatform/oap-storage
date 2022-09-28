package oap.dynamodb.crud;

import oap.dynamodb.Key;
import oap.dynamodb.convertors.DynamodbDatatype;
import oap.dynamodb.restrictions.ReservedWords;
import org.apache.commons.codec.digest.DigestUtils;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DynamoDbHelper {

    protected String generateKeyValue( Key key ) {
        String keyName = key.getColumnName();
        String keyValue = key.getColumnValue();
        if( keyValue.length() <= 256 ) return keyValue;
        byte[] digest = DigestUtils.digest( DigestUtils.getDigest( "SHA-256" ), keyValue.getBytes( StandardCharsets.UTF_8 ) );
        return new String( Base64.getEncoder().encode( digest ), StandardCharsets.UTF_8 );
    }

    protected Map<String, AttributeValue> getKeyAttribute( Key key ) {
        AttributeValue keyAttribute = AttributeValue.builder().s( generateKeyValue( key ) ).build();
        return Collections.singletonMap( key.getColumnName(), keyAttribute );
    }

    @NotNull
    protected Map<String, AttributeValue> generateBinNamesAndValues( Key key,
                                                                     String binName,
                                                                     Object binValue,
                                                                     Map<String, AttributeValue> oldValues ) {
        if ( !ReservedWords.isAttributeNameAppropriate( binName ) ) {
            throw new IllegalArgumentException( "Such attribute name '" + binName + "' is unsupported in DynamoDB, "
                    + "see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html" );
        }
        Map<String, AttributeValue> newValues = new HashMap<>();

        if( oldValues != null ) {
            newValues.putAll( oldValues );
        } else {
            //create an id
            newValues.putAll( getKeyAttribute( key ) );
        }
        if( binValue != null ) {
            newValues.put( binName, DynamodbDatatype.createAttributeValueFromObject( binValue ) );
        } else {
            newValues.remove( binName );
        }
        return newValues;
    }
}
