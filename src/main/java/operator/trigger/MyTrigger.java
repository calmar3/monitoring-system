package operator.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Created by maurizio on 06/04/17.
 */
public class MyTrigger extends Trigger<Object, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private MyTrigger() {}

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onElement");
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            System.out.println("FIRE");
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            System.out.println("CONTINUE");
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        System.out.println("onEventTime: time " + time + " window.MaxTimestamtp " + window.maxTimestamp() + "\n");
        return time == window.maxTimestamp() ?
                TriggerResult.FIRE :
                TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("clear");
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        Trigger.OnMergeContext ctx) {
        ctx.registerEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>
     * Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static MyTrigger create() {
        return new MyTrigger();
    }

}
