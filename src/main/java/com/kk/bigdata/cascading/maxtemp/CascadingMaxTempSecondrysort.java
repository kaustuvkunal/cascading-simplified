package com.kk.bigdata.cascading.maxtemp;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Last;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Kaustuv Kunal
 */
public class CascadingMaxTempSecondrysort
{

    public static void main(String[] args)
    {
        String inputPath = "/Users/hadoop/MaxTempInputSmallFiles";
        Scheme sourceScheme = new TextLine();
        Tap source = new Lfs(sourceScheme, inputPath);

        String outputPath = "/Users/hadoop/MaxTempOutputCascadingSS";
        Tap sink = new Lfs(new TextDelimited(Fields.ALL, ","), outputPath, SinkMode.REPLACE);

        Pipe assembly = new Pipe("Assembler");

        Fields ipMethod = new Fields("offset", "line");
        
        Pipe result = new Each(assembly, ipMethod, new MaxTempFunction(new Fields("year", "temp")), Fields.RESULTS);
        
        result = new GroupBy(result, new Fields("year"), new Fields("temp"));
        result = new Every(result, Fields.ALL, new Last(), Fields.RESULTS);
        FlowConnector flowconnector = new HadoopFlowConnector(getPropertiesForCascadingJob());

        FlowDef flowDef = FlowDef.flowDef()
                .addSource(assembly, source)
                .addTailSink(result, sink);

        flowconnector.connect(flowDef).complete();
    }

    private static Map<Object, Object> getPropertiesForCascadingJob()
    {
        Map<Object, Object> properties = new HashMap<>();

        properties.put("HfsProps.setUseCombinedInput", true);

        return properties;
    }

}
