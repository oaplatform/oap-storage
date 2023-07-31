/*
 * The MIT License (MIT)
 *
 * Copyright (c) Open Application Platform Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package oap.storage.dynamo.client.modifiers.impl;

import oap.storage.dynamo.client.modifiers.CreateTableRequestModifier;
import software.amazon.cryptography.dbencryptionsdk.dynamodb.model.DynamoDbTableEncryptionConfig;
import software.amazon.cryptography.dbencryptionsdk.structuredencryption.model.CryptoAction;
import software.amazon.cryptography.materialproviders.IKeyring;
import software.amazon.cryptography.materialproviders.model.DBEAlgorithmSuiteId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CreateTableWithEncryptionRequestModifier implements CreateTableRequestModifier {
    private static Map<String, DynamoDbTableEncryptionConfig> tablesConfig = new HashMap<>();

    private final IKeyring kmsKeyring;
    private final String tableName;
    private final String partitionKeyName;
    private final String sortKeyName;

    //      When defining your DynamoDb schema and deciding on attribute names,
    //      choose a distinguishing prefix (such as ":") for all attributes that
    //      you do not want to include in the signature.
    //      This has two main benefits:
    //      - It is easier to reason about the security and authenticity of data within your item
    //        when all unauthenticated data is easily distinguishable by their attribute name.
    //      - If you need to add new unauthenticated attributes in the future,
    //        you can easily make the corresponding update to your `attributeActionsOnEncrypt`
    //        and immediately start writing to that new attribute, without
    //        any other configuration update needed.
    //      Once you configure this field, it is not safe to update it.
    private String unsignedAttributePrefix = ":";

    // Specifying an algorithm suite is not required,
    // but is done here to demonstrate how to do so.
    // We suggest using the
    // `ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY_ECDSA_P384_SYMSIG_HMAC_SHA384` suite,
    // which includes AES-GCM with key derivation, signing, and key commitment.
    // This is also the default algorithm suite if one is not specified in this config.
    // For more information on supported algorithm suites, see:
    //   https://docs.aws.amazon.com/database-encryption-sdk/latest/devguide/supported-algorithms.html
    private DBEAlgorithmSuiteId algorithmSuiteId = DBEAlgorithmSuiteId.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY_ECDSA_P384_SYMSIG_HMAC_SHA384;

    private Map<String, CryptoAction> attributeActionsOnEncrypt = new HashMap<>();

    public CreateTableWithEncryptionRequestModifier( IKeyring kmsKeyring, String tableName, String partitionKeyName, String sortKeyName ) {
        Objects.requireNonNull( kmsKeyring );
        Objects.requireNonNull( tableName );
        Objects.requireNonNull( partitionKeyName );
        this.kmsKeyring = kmsKeyring;
        this.tableName = tableName;
        this.partitionKeyName = partitionKeyName;
        this.sortKeyName = sortKeyName;

        attributeActionsOnEncrypt.put( partitionKeyName, CryptoAction.SIGN_ONLY); // Our partition attribute must be SIGN_ONLY
        if ( sortKeyName != null ) {
            attributeActionsOnEncrypt.put( sortKeyName, CryptoAction.SIGN_ONLY ); // Our sort attribute must be SIGN_ONLY
        }
    }

    @Override
    public void accept( CreateTableDescriber describer ) {
        DynamoDbTableEncryptionConfig config = DynamoDbTableEncryptionConfig.builder()
            .logicalTableName( describer.getTableName() )
            .partitionKeyName( partitionKeyName )
            .sortKeyName( sortKeyName )
            .attributeActionsOnEncrypt( attributeActionsOnEncrypt )
            .keyring( kmsKeyring )
            .allowedUnsignedAttributePrefix( unsignedAttributePrefix )
            .algorithmSuiteId( algorithmSuiteId )
            .build();
        tablesConfig.put( tableName, config );
    }

    /**
     * Call this method to specify which columns should be signed/encrypted
     * or left non-encrypted at all.
     * Note: partitioning key and order column key SHOULD not be included here as
     * they've already described as SIGN_ONLY
     * NOTE: DO_NOTHING action should be assigned for non-encrypted columns,
     * they columns should have been started with 'unsignedAttributePrefix'
     * @param columnName is attribute name
     * @param action one of the action: ENCRYPT_AND_SIGN for encrypted, SIGN_ONLY for signed, and DO_NOTHING for the rest.
     */
    public void addColumnAction( String columnName, CryptoAction action ) {
        if ( action == CryptoAction.DO_NOTHING && !columnName.startsWith( unsignedAttributePrefix ) ) {
            throw new IllegalArgumentException( "You specified column prefix '" + unsignedAttributePrefix + "' for non-encrypted attribute, but this prefix is missing for given attribute '" + columnName + "'" );
        }
        attributeActionsOnEncrypt.put( columnName, action );
    }

    public static Map<String, DynamoDbTableEncryptionConfig> getEncryptionTablesConfig() {
        return new HashMap<>( tablesConfig );
    }
}
