package oap.dynamodb.creator;

import lombok.extern.slf4j.Slf4j;
import oap.dynamodb.annotations.API;
import oap.dynamodb.convertors.DynamodbDatatype;

import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


import static oap.dynamodb.convertors.DynamodbDatatype.MAP;
import static oap.dynamodb.convertors.DynamodbDatatype.NUMBER;
import static oap.dynamodb.convertors.DynamodbDatatype.SET_OF_BINARIES;
import static oap.dynamodb.convertors.DynamodbDatatype.SET_OF_NUMBERS;
import static oap.dynamodb.convertors.DynamodbDatatype.SET_OF_STRINGS;
import static oap.dynamodb.convertors.DynamodbDatatype.fromAttributeValue;

/**
 * Creates a Bean class from attribute values for it.
 */
@Slf4j
public class PojoBeanFromDynamoCreator<T> {

    /**
     * Behaves like StaticImmutableTableSchema but if there is no Table Schema. It tries to set up
     * all fields in a given class according to their name and class, from a given values, which usually can be got from
     * DynamoDB table.
     * @param clazz to make a new instance
     * @param values fields to be set up
     * @return a new instance of desired class with all fields are set from given values
     * @throws ReflectiveOperationException
     *
     * @Note: StaticImmutableTableSchema could be created by table name or by class, if that class is mapped to table
     * ( see DynamoDB annotations)
     */
    @API
    public T createBean( Class<? extends T> clazz, Map<String, AttributeValue> values ) {
        Field processingField = null;
        try {
            T result = createInstanceOfBeanClass( clazz, values );
            // set up fields
            for ( Field field : clazz.getDeclaredFields() ) {
                if ( Modifier.isStatic( field.getModifiers() ) ) {
                    // cannot setup static
                    continue;
                }
                if ( Modifier.isFinal( field.getModifiers() ) ) {
                    //was set up earlier in Constructor
                    continue;
                }
                processingField = field;
                try {
                    Method getter = PojoBeanToDynamoCreator.findGetterMethodForField( field, clazz );
                    if ( getter.getAnnotation( DynamoDbIgnore.class ) != null ) {
                        //skipping field with @DynamoDbIgnore anno
                        continue;
                    }
                } catch ( NoSuchMethodException ex ) {
                    // ignore this exception as we do not care of missing annotation, just do further
                }
                Object value = prepareValue( values, field );
                if ( value == null ) continue;
                if ( Modifier.isPublic( field.getModifiers() ) ) {
                    field.set( result, value );
                    continue;
                }
                //look for the setter method
                callSetMethod( result, field, value );
            }
            return result;
        } catch ( IllegalArgumentException | ReflectiveOperationException ex ) {
            throw new RuntimeException( "Cannot set field '" + processingField.getName() + "' in class '"
                    + processingField.getDeclaringClass().getCanonicalName() + "'", ex );
        }
    }

    @Nullable
    T createInstanceOfBeanClass( Class<? extends T> clazz, Map<String, AttributeValue> values ) throws ReflectiveOperationException {
        Objects.requireNonNull( clazz );
        Objects.requireNonNull( values );
        List<Field> finalFields = new ArrayList<>();
        //final fields look up
        for ( Field field : clazz.getDeclaredFields() ) {
            if ( Modifier.isStatic( field.getModifiers() ) ) {
                // cannot setup static
                continue;
            }
            if ( Modifier.isFinal( field.getModifiers() ) ) {
                //set up in Constructor
                finalFields.add( field );
            }
        }
        T result = null;
        if ( finalFields.isEmpty() ) {
            try {
                result = clazz.getDeclaredConstructor().newInstance();
                return result;
            } catch ( IllegalAccessException ex ) {
                throw new ReflectiveOperationException( "No public constructor found in class '" + clazz.getCanonicalName() + "'", ex );
            }
        }
        for ( Constructor constructor : clazz.getDeclaredConstructors() ) {
            //check length of arguments
            if ( constructor.getParameterTypes().length != finalFields.size() ) {
                continue;
            }
            //check types
            List<Object> constructorValues = new ArrayList<>();
            boolean fits = true;
            for( int i = 0; i < constructor.getParameterTypes().length; i++ ) {
                Field field = finalFields.get( i );
                if ( constructor.getParameterTypes()[ i ] != field.getGenericType() ) {
                    fits = false;
                    break;
                }
                AttributeValue attributeValue = values.get( field.getName() );
                DynamodbDatatype datatype = DynamodbDatatype.of( field.getType() );
                Object value = fromAttributeValue( datatype, attributeValue, null );
                if ( datatype == NUMBER ) {
                    value = getValueForPrimitiveType( field, value );
                }
                constructorValues.add( value );
            }
            if ( fits ) {
               result = ( T ) constructor.newInstance( constructorValues.toArray() );
               break;
            }
        }
        if ( result == null ) throw new NoSuchMethodException( "Cannot find public no-args constructor for class " + clazz.getCanonicalName() );
        return result;
    }

    private void callSetMethod( T result, Field field, Object valueArg ) throws ReflectiveOperationException {
        boolean setterInvoked = false;
        try {
            Object value = valueArg;
            if ( field.getType().isPrimitive() ) {
                value = getValueForPrimitiveType( field, valueArg );
            }
            Method method = result.getClass().getMethod( "set" + toCapitalize( field.getName() ), field.getType() );
            if ( method.canAccess( result ) ) {
                method.invoke( result, value );
                setterInvoked = true;
            }
        } catch ( NoSuchMethodException ex ) {
            boolean exceptionLogged = false;
            for ( Method method : result.getClass().getDeclaredMethods() ) {
                if ( method.canAccess( result )
                        && method.getName().startsWith( "set" )
                        && method.getReturnType().getCanonicalName().equals( "void" )
                        && method.getName().equals( "set" + toCapitalize( field.getName() ) ) ) {
                    //candidate
                    try {
                        method.invoke( result, valueArg );
                        setterInvoked = true;
                    } catch ( Exception pex ) {
                        log.debug( "Cannot find appropriate setter for field '{}':", field.getName(), ex.getMessage() );
                    }
                    exceptionLogged = true;
                    break;
                }
            }
            if ( !exceptionLogged ) {
                log.debug( "Cannot find appropriate setter for field '{}':", field.getName(), ex.getMessage() );
            }
        }
        if ( !setterInvoked ) {
            throw new NoSuchMethodException( "Setter method for field " + field.getName() + " in class " + field.getDeclaringClass().getCanonicalName() + " is not found" );
        }
    }

    public static Object getValueForPrimitiveType( Field field, Object valueArg ) {
        if ( field.getType() == int.class || field.getType() == Integer.class ) {
            if ( valueArg instanceof Integer ) return valueArg;
            return ( ( Double ) valueArg ).intValue();
        }
        if ( field.getType() == long.class || field.getType() == Long.class ) {
            if ( valueArg instanceof Long ) return valueArg;
            return ( ( Double ) valueArg ).longValue();
        }
        if ( field.getType() == float.class || field.getType() == Float.class ) {
            if ( valueArg instanceof Float ) return valueArg;
            return ( ( Double ) valueArg ).floatValue();
        }
        if ( field.getType() == double.class || field.getType() == Double.class ) {
            return ( ( Double ) valueArg ).doubleValue();
        }
        if ( field.getType() == boolean.class || field.getType() == Boolean.class ) {
            return ( ( Boolean ) valueArg ).booleanValue();
        }
        return valueArg;
    }

    private String toCapitalize( String name ) {
        if ( name.length() == 1 ) {
            return name.toUpperCase( Locale.ROOT );
        }
        if ( name.length() > 1 ) {
            return name.substring( 0, 1 ).toUpperCase( Locale.ROOT ) + name.substring( 1 );
        }
        throw new IllegalArgumentException( "Cannot find setter for '" + name + "'" );
    }

    private Object prepareValue( Map<String, AttributeValue> values, Field field ) {
        DynamodbDatatype dynamodbDatatype = null;
        if ( field.getGenericType().toString().equals( "java.util.Set<java.lang.String>" ) ) {
            dynamodbDatatype = SET_OF_STRINGS;
        } else if ( field.getGenericType().toString().equals( "java.util.List<java.lang.String>" ) ) {
            dynamodbDatatype = SET_OF_STRINGS;
        } else if ( field.getGenericType().toString().equals( "java.util.List<byte[]>" ) ) {
            dynamodbDatatype = SET_OF_BINARIES;
        } else if ( field.getGenericType().toString().equals( "java.util.Set<byte[]>" ) ) {
            dynamodbDatatype = SET_OF_BINARIES;
        } else if ( field.getGenericType().toString().startsWith( "java.util.List<java.lang." ) ) {
            dynamodbDatatype = SET_OF_NUMBERS;
        } else if ( field.getGenericType().toString().startsWith( "java.util.Set<java.lang." ) ) {
            dynamodbDatatype = SET_OF_NUMBERS;
        } else if ( field.getGenericType().toString().startsWith( "java.util.Map<java.lang.String," ) ) {
            dynamodbDatatype = MAP;
        } else {
            dynamodbDatatype = DynamodbDatatype.of( ( Class ) field.getGenericType() );
            if ( dynamodbDatatype == MAP ) {
                AttributeValue value = values.get( field.getName() );
                return new PojoBeanFromDynamoCreator().createBean( field.getType(), value.m() );
            }
        }
        Object value = fromAttributeValue( dynamodbDatatype, values.get( field.getName() ), null );
        if( value != null && dynamodbDatatype == NUMBER ) {
            Object convertedValue = getNumberClass( field, value );
            if ( convertedValue != null ) return convertedValue;
        } else if( value != null && dynamodbDatatype == SET_OF_NUMBERS ) {
            if ( field.getGenericType().toString().startsWith( "java.util.List<java.lang.Integer>" ) ) {
                value = ( ( List<Double> ) value ).stream().map( v -> v.intValue() ).collect( Collectors.toList() );
            } else if ( field.getGenericType().toString().startsWith( "java.util.List<java.lang.Long>" ) ) {
                value = ( ( List<Double> ) value ).stream().map( v -> v.longValue() ).collect( Collectors.toList() );
            } else if ( field.getGenericType().toString().startsWith( "java.util.List<java.lang.Float>" ) ) {
                value = ( ( List<Double> ) value ).stream().map( v -> v.floatValue() ).collect( Collectors.toList() );
            } else if ( field.getGenericType().toString().startsWith( "java.util.List<java.lang.Double>" ) ) {
                value = ( ( List<Double> ) value ).stream().map( v -> v.doubleValue() ).collect( Collectors.toList() );
            } else if ( field.getGenericType().toString().startsWith( "java.util.Set<java.lang.Integer" ) ) {
                value = ( ( List<Double> ) value ).stream().map( v -> v.intValue() ).collect( Collectors.toSet() );
            } else if ( field.getGenericType().toString().startsWith( "java.util.Set<java.lang.Long>" ) ) {
                value = ( ( List<Double> ) value ).stream().map( v -> v.longValue() ).collect( Collectors.toSet() );
            } else if ( field.getGenericType().toString().startsWith( "java.util.Set<java.lang.Float>" ) ) {
                value = ( ( List<Double> ) value ).stream().map( v -> v.floatValue() ).collect( Collectors.toSet() );
            } else if ( field.getGenericType().toString().startsWith( "java.util.Set<java.lang.Double>" ) ) {
                value = ( ( List<Double> ) value ).stream().map( v -> v.doubleValue() ).collect( Collectors.toSet() );
            }

        }
        return value;
    }

    @Nullable
    private Object getNumberClass( Field field, Object value ) {
        String name = ( ( Class<?> ) field.getGenericType() ).getName();
        if ( "java.lang.Long".equals( name ) || "long".equals( name ) ) {
            return ( ( Double ) value ).longValue();
        }
        if ( "java.lang.Integer".equals( name ) || "int".equals( name ) ) {
            return ( ( Double ) value ).intValue();
        }
        if ( "java.lang.Float".equals( name ) || "float".equals( name ) ) {
            return ( ( Double ) value ).floatValue();
        }
        if ( "java.lang.Double".equals( name ) || "double".equals( name ) )  {
            return value;
        }
        if ( "java.lang.Number".equals( name ) ) {
            return value;
        }
        return null;
    }

    public T fromDynamo( Class<? extends T> clazz, Map<String, AttributeValue> values ) {
        TableSchema schema = TableSchema.fromClass( clazz );
        return ( T ) schema.mapToItem( values );
    }
}
