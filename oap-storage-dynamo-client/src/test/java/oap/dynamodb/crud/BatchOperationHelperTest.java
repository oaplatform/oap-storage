package oap.dynamodb.crud;

import com.google.common.collect.Lists;
import oap.dynamodb.batch.WriteBatchOperationHelper;
import oap.testng.Fixtures;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;

public class BatchOperationHelperTest extends Fixtures {
    private WriteBatchOperationHelper helper;

    @BeforeMethod
    public void setUp() {
        helper = new WriteBatchOperationHelper( null ) {
            public String toString() {
                return operations.toString();
            }
        };
    }

    @Test
    public void testRearrangeByOne() {
        helper.setBatchSize( 1 );
        List<AbstractOperation> operations = createOperations( false );

        helper.addOperations( operations );

        assertEquals( 12, helper.getOperations().size() );
        assertEquals( "[[C1], [C2], [U1], [C3], [U2], [D1], [C4], [D2], [D4], [U3], [U4], [U5]]", helper.toString() );
    }

    @Test
    public void testRearrangeByThree() {
        helper.setBatchSize( 3 );
        List<AbstractOperation> operations = createOperations( false );

        helper.addOperations( operations );

        assertEquals( 9, helper.getOperations().size() );
        assertEquals( "[[C1, C2], [U1], [C3], [U2], [D1, C4, D2], [D4], [U3], [U4], [U5]]", helper.toString() );
    }

    @Test
    public void testRearrangeBySix() {
        helper.setBatchSize( 6 );
        List<AbstractOperation> operations = createOperations( false );

        helper.addOperations( operations );

        assertEquals( 8, helper.getOperations().size() );
        assertEquals( "[[C1, C2], [U1], [C3], [U2], [D1, C4, D2, D4], [U3], [U4], [U5]]", helper.toString() );
    }

    @Test
    public void testRearrangeByOneSeparate() {
        helper.setBatchSize( 1 );

        createOperations( false ).stream().forEach( helper::addOperation );

        assertEquals( 12, helper.getOperations().size() );
        assertEquals( "[[C1], [C2], [U1], [C3], [U2], [D1], [C4], [D2], [D4], [U3], [U4], [U5]]", helper.toString() );
    }

    @Test
    public void testRearrangeByThreeSeparate() {
        helper.setBatchSize( 3 );

        createOperations( false ).stream().forEach( helper::addOperation );

        assertEquals( 9, helper.getOperations().size() );
        assertEquals( "[[C1, C2], [U1], [C3], [U2], [D1, C4, D2], [D4], [U3], [U4], [U5]]", helper.toString() );
    }

    @Test
    public void testRearrangeBySixSeparate() {
        helper.setBatchSize( 6 );

        createOperations( false ).stream().forEach( helper::addOperation );

        assertEquals( 8, helper.getOperations().size() );
        assertEquals( "[[C1, C2], [U1], [C3], [U2], [D1, C4, D2, D4], [U3], [U4], [U5]]", helper.toString() );
    }

    @Test
    public void testRearrangeAllInOne() {
        helper.setBatchSize( 7 );

        createOperations( true ).stream().forEach( helper::addOperation );

        assertEquals( 1, helper.getOperations().size() );
        assertEquals( "[[C1, C2, C3, D1, C4, D2, D4]]", helper.toString() );
    }

    @Test
    public void testRearrangeAllInTwo() {
        helper.setBatchSize( 6 );

        createOperations( true ).stream().forEach( helper::addOperation );

        assertEquals( 2, helper.getOperations().size() );
        assertEquals( "[[C1, C2, C3, D1, C4, D2], [D4]]", helper.toString() );
    }

    private List<AbstractOperation> createOperations( boolean addOnlyCreateDelete ) {
        if ( addOnlyCreateDelete ) {
            return Lists.newArrayList(
                    new CreateItemOperation( "C1" ),
                    new CreateItemOperation( "C2" ),
                    new CreateItemOperation( "C3" ),
                    new DeleteItemOperation( "D1" ),
                    new CreateItemOperation( "C4" ),
                    new DeleteItemOperation( "D2" ),
                    new DeleteItemOperation( "D4" )
            );
        }
        return Lists.newArrayList(
                new CreateItemOperation( "C1" ),
                new CreateItemOperation( "C2" ),
                new UpdateItemOperation( "U1" ),
                new CreateItemOperation( "C3" ),
                new UpdateItemOperation( "U2" ),
                new DeleteItemOperation( "D1" ),
                new CreateItemOperation( "C4" ),
                new DeleteItemOperation( "D2" ),
                new DeleteItemOperation( "D4" ),
                new UpdateItemOperation( "U3" ),
                new UpdateItemOperation( "U4" ),
                new UpdateItemOperation( "U5" )
        );
    }
}
