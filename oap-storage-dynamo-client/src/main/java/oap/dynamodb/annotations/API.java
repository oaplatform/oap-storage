package oap.dynamodb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention( RetentionPolicy.SOURCE )
@Target( {ElementType.TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD } )
public @interface API {
    String since() default "1.0";
}
