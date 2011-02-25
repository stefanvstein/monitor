(ns agent.Transformer
  (:import [clojure.asm MethodAdapter ClassAdapter Opcodes ClassReader ClassWriter])
  (:gen-class
   :implements [java.lang.instrument.ClassFileTransformer]))

(defn -init [methods])

(defn create-method-adapter [method-visitor class-name method-name]
  (proxy [MethodAdapter] [method-visitor]
    (visitCode []
	       (proxy-super visitLdcInsn class-name)
	       (proxy-super visitLdcInsn method-name)
	       (proxy-super visitMethodInsn Opcodes/INVOKESTATIC "agent/Profiler" "start" "(Ljava/lang/String;Ljava/lang/String;)V")
	       (proxy-super visitCode))
    (visitInsn [instruction]
	       (when (or (= instruction Opcodes/ARETURN)
			 (= instruction Opcodes/DRETURN)
			 (= instruction Opcodes/FRETURN)
			 (= instruction Opcodes/IRETURN)
			 (= instruction Opcodes/LRETURN)
			 (= instruction Opcodes/RETURN)
			 (= instruction Opcodes/ATHROW))
		 (proxy-super visitLdcInsn class-name)
		 (proxy-super visitLdcInsn method-name)
		 (proxy-super visitMethodInsn Opcodes/INVOKESTATIC "agent/Profiler" "end" "(Ljava/lang/String;Ljava/lang/String;)V"))  
			      
	       (proxy-super visitInsn instruction))))

(defn create-adapter [writer class-name]
  (proxy [ClassAdapter] [writer]
    (visitMethod [arg name descriptor signature exceptions]
		 (let [mv (proxy-super visitMethod arg name descriptor signature exceptions)]
		   (create-method-adapter mv class-name name)))))

(defn -transform  [self class-loader, class-name, class-being-redefined, protection-domain, byte-array]
 
  (if-not (.startsWith class-name "agent/")
    (try (.loadClass class-loader "agent.Transformer")
	 (.println System/out "redefining")
	 (let [reader (ClassReader. byte-array)
	       writer (ClassWriter. true)
	       adapter (create-adapter writer class-name)]
	   (.accept reader adapter true)
	   (.toByteArray writer))
	 (catch ClassNotFoundException _
	   byte-array))
    byte-array))

