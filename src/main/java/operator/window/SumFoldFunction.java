package operator.window;


import model.Lamp;
import model.Street;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.omg.CORBA.Object;

/**
 * Created by maurizio on 04/04/17.
 *
 * Preso un flusso esterno di Lamp restituisce al termine della finestra una Tuple2 contenente un Lamp avente
 * nell'attributo consumption la somma di tutti i Lamp arrivati all'interno della finestra
 */
public class SumFoldFunction implements FoldFunction<Object, Tuple2<Object, Long>> {

    @Override
    public Tuple2<Object, Long> fold(Tuple2<Object, Long> in, Object obj) throws Exception {
        if(in.f0 != null) {
            //System.out.println("new Consumption " + ((Lamp)obj).getConsumption() + " Timestamp " + ((Lamp)obj).getTimestamp());
            //System.out.println("old Consumption " + in.f0.getConsumption() + " Timestamp " + in.f0.getTimestamp() + " number " + in.f1);

            if(obj instanceof Lamp) {
                Lamp l = (Lamp) obj;
                return new Tuple2<>((Object)(new Lamp(l.getLampId(), ((Lamp)in.f0).getConsumption() + l.getConsumption(), l.getAddress(), l.getTimestamp())), in.f1 + 1);
            }
            else /*(obj instanceof Street)*/ {
                Street s = (Street) obj;
                return new Tuple2<>((Object)(new Street(s.getId(), ((Street)in.f0).getConsumption() + s.getConsumption(), s.getTimestamp())) , in.f1 + 1);
            }
        }
        else {
            //System.out.println("Object in new window");
            //System.out.println("Consumption " + in.f0.getConsumption() + " Timestamp " + in.f0.getTimestamp() + " number " + in.f1);
            return new Tuple2<>(obj, (long)1);
        }
    }
}
