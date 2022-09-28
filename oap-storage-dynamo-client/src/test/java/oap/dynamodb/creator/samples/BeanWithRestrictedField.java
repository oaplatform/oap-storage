package oap.dynamodb.creator.samples;


import lombok.ToString;

@ToString
public class BeanWithRestrictedField {
    public String id;
    public String name;
    public int c;

    public BeanWithRestrictedField( String id, String name ) {
        this( name );
        this.id = id;
    }

    BeanWithRestrictedField( String name ) {
        this.name = name;
    }

    public BeanWithRestrictedField() {
    }

    public int getC() {
        return c;
    }

    public void setC( int c ) {
        this.c = c;
    }

    public String getId() {
        return id;
    }

    public void setId( String id ) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName( String name ) {
        this.name = name;
    }
}
