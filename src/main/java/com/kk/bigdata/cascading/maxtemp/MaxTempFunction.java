package com.kk.bigdata.cascading.maxtemp;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 * @author Kaustuv Kunal
 */
public class MaxTempFunction extends BaseOperation implements Function
{

    public MaxTempFunction(Fields fieldDeclaration)
    {
        super(2, fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall)
    {
        // get the arguments TupleEntry
        TupleEntry arguments = functionCall.getArguments();
        String line = arguments.getString(1);

        String year = line.substring(4, 9);
        float temp = Float.parseFloat(line.substring(38, 43));

        Tuple result = new Tuple();

        result.addString(year);
        result.addFloat(temp);

        functionCall.getOutputCollector().add(result);
    }
}
