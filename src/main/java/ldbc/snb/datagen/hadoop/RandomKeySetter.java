package ldbc.snb.datagen.hadoop;

import ldbc.snb.datagen.objects.Person;

/**
 * Created by aprat on 11/17/14.
 */
public class RandomKeySetter implements HadoopFileKeyChanger.KeySetter<TupleKey> {

    public TupleKey getKey(Object object ) {
        Person person = (Person)object;
        return new TupleKey(person.randomId(),person.accountId());
    }
}

