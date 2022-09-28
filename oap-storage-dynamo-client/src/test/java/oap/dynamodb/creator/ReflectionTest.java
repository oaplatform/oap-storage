package oap.dynamodb.creator;

import oap.dynamodb.creator.samples.FinalFieldInArgsConstructor;
import oap.dynamodb.creator.samples.NoDefaultConstructor;
import oap.dynamodb.creator.samples.NoPublicConstructor;
import oap.dynamodb.creator.samples.SeveralDifferentFinalFieldInArgsConstructor;
import oap.dynamodb.creator.samples.SeveralFinalFieldInArgsConstructor;
import oap.testng.Fixtures;
import oap.util.Maps;
import oap.util.Pair;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ReflectionTest extends Fixtures {

    @Test
    public void missingPublicConstructor() throws Exception {
        PojoBeanFromDynamoCreator<NoPublicConstructor> creator = new PojoBeanFromDynamoCreator<>();
        assertThrows( ReflectiveOperationException.class,
                () -> creator.createInstanceOfBeanClass( NoPublicConstructor.class, Collections.emptyMap() ) );
    }

    @Test
    public void missingNoArgsConstructor() throws Exception {
        PojoBeanFromDynamoCreator<NoDefaultConstructor> creator = new PojoBeanFromDynamoCreator<>();
        assertThrows( ReflectiveOperationException.class,
                () -> creator.createInstanceOfBeanClass( NoDefaultConstructor.class, Collections.emptyMap() ) );
    }

    @Test
    public void existsOnlyFinalArgConstructor() throws Exception {
        PojoBeanFromDynamoCreator<FinalFieldInArgsConstructor> creator = new PojoBeanFromDynamoCreator<>();
        FinalFieldInArgsConstructor instance = creator.createInstanceOfBeanClass( FinalFieldInArgsConstructor.class,
                Collections.singletonMap( "finalField", AttributeValue.fromS( "value" ) ) );

        assertEquals( "value", instance.getFinalField() );
    }

    @Test
    public void existsOnlyFinalArgsConstructor() throws Exception {
        PojoBeanFromDynamoCreator<SeveralFinalFieldInArgsConstructor> creator = new PojoBeanFromDynamoCreator<>();
        SeveralFinalFieldInArgsConstructor instance = creator.createInstanceOfBeanClass( SeveralFinalFieldInArgsConstructor.class,
                Maps.of(
                    new Pair<>( "finalField1", AttributeValue.fromS( "value1" ) ),
                    new Pair<>( "finalField2", AttributeValue.fromS( "value2" ) ),
                    new Pair<>( "finalField3", AttributeValue.fromS( "value3" ) )
                )
        );

        assertEquals( "value1", instance.getFinalField1() );
        assertEquals( "value2", instance.getFinalField2() );
        assertEquals( "value3", instance.getFinalField3() );
    }

    @Test
    public void existsOnlyFinalDifferentTypesPrimitivesArgsConstructor() throws Exception {
        PojoBeanFromDynamoCreator<SeveralDifferentFinalFieldInArgsConstructor> creator = new PojoBeanFromDynamoCreator<>();
        SeveralDifferentFinalFieldInArgsConstructor instance = creator.createInstanceOfBeanClass( SeveralDifferentFinalFieldInArgsConstructor.class,
                Maps.of(
                        new Pair<>( "finalField1", AttributeValue.fromS( "value1" ) ),
                        new Pair<>( "finalField2", AttributeValue.fromBool( true ) ),
                        new Pair<>( "finalField3", AttributeValue.fromN( "123456789" ) )
                )
        );

        assertEquals( "value1", instance.getFinalField1() );
        assertEquals( true, instance.isFinalField2() );
        assertEquals( 123456789, instance.getFinalField3().intValue() );
    }


}
