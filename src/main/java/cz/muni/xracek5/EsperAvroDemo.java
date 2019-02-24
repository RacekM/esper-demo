package cz.muni.xracek5;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@SuppressWarnings(value = "All")
public class EsperAvroDemo {
    private static final String MODULE_TEXT =
            "create table TopKTable (wordsCnt countMinSketch({\n" +
                    "  epsOfTotalCount: 0.000002,\n" +
                    "  confidence: 0.999,\n" +
                    "  seed: 38576,\n" +
                    "  topk: 3\n" +
                    "}));\n" +
                    "into table TopKTable\n" +
                    "select countMinSketchAdd(symbol) as wordsCnt\n" +
                    "from StockTick;\n" +
                    "\n" +
                    "insert into NumericStatsEvent\n" +
                    "select sum(price) as price\n" +
                    "from StockTick#length_batch(3);\n" +
                    "\n" +
                    "@Name('Out')\n" +
                    "on NumericStatsEvent\n" +
                    "insert into ResultEvent\n" +
                    "select delete NumericStatsEvent.price as price ,\n" +
                    "          wordsCnt.countMinSketchTopk() as topK\n" +
                    "from TopKTable";

    public static void main(String[] args) throws EPDeployException, EPCompileException, InterruptedException {
        Configuration conf = new Configuration();
        Map<String, Object> stockTick = new HashMap();
        stockTick.put("symbol", String.class);
        stockTick.put("price", Integer.class);
        conf.getCommon().addEventType("StockTick", stockTick);


        Random random = new Random();
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(conf);
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        CompilerArguments compilerArgs = new CompilerArguments();
        compilerArgs.setConfiguration(conf);
        EPCompiled compile = compiler.compile(MODULE_TEXT, compilerArgs);
        EPDeployment deploy = runtime.getDeploymentService().deploy(compile);

        for (EPStatement statement : deploy.getStatements()) {
            if (statement.getName().equals("Out")) {
                statement.addListener(new ConsoleListener());
            }
        }

        while (true) {
            Thread.sleep(1000);
            Map<String, Object> event = new HashMap();
            event.put("symbol", String.valueOf(random.nextInt() % 10));
            event.put("price", random.nextInt());
            runtime.getEventService().getEventSender("StockTick").sendEvent(event);
        }

    }

}
