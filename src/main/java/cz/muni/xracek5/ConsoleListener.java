package cz.muni.xracek5;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;

public class ConsoleListener implements UpdateListener {

    public void update(EventBean[] eventBeans, EventBean[] eventBeans1, EPStatement epStatement, EPRuntime epRuntime) {
        if (eventBeans != null) {
            for (EventBean eventBean : eventBeans) {
                System.out.println(eventBean.getUnderlying());
            }
        }
    }

}
