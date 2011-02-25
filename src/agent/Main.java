package agent;
import java.lang.instrument.Instrumentation;

public class Main {
    public static void premain(String args, Instrumentation inst) throws Exception{
	inst.addTransformer((java.lang.instrument.ClassFileTransformer) (agent.Main.class.getClassLoader().loadClass("agent.Transformer")).newInstance());
    }
}