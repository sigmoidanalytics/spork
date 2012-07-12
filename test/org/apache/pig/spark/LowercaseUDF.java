package org.apache.pig.spark;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * @author billg
 */
public class LowercaseUDF extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        return ((String)input.get(0)).toLowerCase();
    }
}
