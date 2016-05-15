/**
 * 
 */
package org.qcri.truthdiscovery.fullyscalable.truthfinder;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.qcri.rheem.core.data.DataSet;
import org.qcri.rheem.core.data.KeyValuePair;
import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.engine.CacheDataAgent;
import org.qcri.rheem.core.engine.DataAgent;
import org.qcri.rheem.core.engine.LoopDataAgent;
import org.qcri.rheem.core.engine.RheemJob;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;
import org.qcri.rheem.core.util.FlatMapOptions;
import org.qcri.rheem.core.util.MapOptions;
import org.qcri.rheem.truthdiscovery.logical.Compute;
import org.qcri.rheem.truthdiscovery.logical.ComputeWrapper;
import org.qcri.rheem.truthdiscovery.logical.LocalStaging;
import org.qcri.rheem.truthdiscovery.logical.LocalStagingWrapper;
import org.qcri.rheem.truthdiscovery.logical.Loop;
import org.qcri.rheem.truthdiscovery.logical.LoopWrapper;
import org.qcri.rheem.truthdiscovery.logical.Transform;
import org.qcri.rheem.truthdiscovery.logical.TransformWrapper;
import org.qcri.rheem.truthdiscovery.logical.Update;
import org.qcri.rheem.truthdiscovery.logical.UpdateWrapper;
import org.qcri.truthdiscovery.scalable.truthfinder.logical.ScalableTruthFinderMerger;
import org.qcri.truthdiscovery.util.dataModel.data.Source;
import org.qcri.truthdiscovery.util.dataModel.data.SourceClaim;
import org.qcri.truthdiscovery.util.dataModel.data.ValueBucket;
import scala.Tuple2;

/**
 * @author mlba
 * @created date: Jan 24, 2016
 * Scalable Truth Finder Rheem Plan
 * 
 * @Input: Dataset containing the claims, algorithm's initial parameters, 
 * and the set of logical operators that compose the execution plan 
 * @Output: 
 */
public class FullyScalableTruthFinderRheemPlan extends RheemJob implements Serializable {

	protected Transform<?, ?> transformOp;
	protected LocalStaging initOp;
	protected Compute<?, ?> confidenceOp;
	protected Update<?, ?> trustOp;
	protected Loop<?> convergenceOp;
	

	private ArrayList<Double> params ;
	private double startTrust ;
	private double startConfidence;
	
	protected ScalableTruthFinderMerger merger;
	
	/**
	 * @constructor
	 * @param dataset
	 */
	protected FullyScalableTruthFinderRheemPlan(DataSet<?, ?, ?> dataset)
	{
		super(dataset);
		// TODO Auto-generated constructor stub
	}
	public FullyScalableTruthFinderRheemPlan()
	{
		super(null) ;
	}
	
	/**
	 * 
	 * @param dataset
	 * @param params
	 * @param startTrust
	 * @param startConfidence
	 */
	public FullyScalableTruthFinderRheemPlan(DataSet<?,?,?> dataset, ArrayList<Double> params, double startTrust, double startConfidence)
	{
		super(dataset) ;
		this.setParams(params) ;
		this.setStartTrust(startTrust)         ;
		this.setStartConfidence(startConfidence)    ;
	}
	public void setMerger(ScalableTruthFinderMerger merger) { this.merger = merger; }
	long start_time ;
	@Override
	public void run()
	{
		// TODO Auto-generated method stub
		start_time = System.currentTimeMillis();
		
		RheemContext<?> context = dataSet.getDataSets().createContext();
		context.setGlobalVariableMerger(merger);
		
		try 
		{
			DataAgent<?> transformAgent = CacheDataAgent.createAgent(dataSet, context)
			        .map(new TransformWrapper(transformOp), MapOptions.PAIR_OUT)
			        .reduceByKey( new ReduceByKeyWrapper() )
			        .contextUpdate(new LocalStagingWrapper(initOp))
			        .evaluate();
			
			DataSet<?,?,?> transformedDataset = transformAgent.getDataSet() ;
		
			LoopDataAgent<?> finalAgent = LoopDataAgent.createAgent(transformedDataset, context, new LoopWrapper(convergenceOp))
	            	   .map(new ComputeWrapper(confidenceOp), MapOptions.PAIR_OUT)  
	            	   .flatMap(new transformLocal(), FlatMapOptions.PAIR_OUT) //flattening the output of compute
	            	   .reduce(new ReduceByKeyWrapper1())
	                 .map(new UpdateWrapper(trustOp))
	                 .evaluate();
	                 
	       
			
			System.out.println("Final time:" + (System.currentTimeMillis() - start_time));
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	/**
	 * 
	 * @author mlba
	 * Group claims by data item key
	 * for input data bucketing 
	 */
	static class ReduceByKeyWrapper extends LogicalOperatorWrapper<KeyValuePair<Integer, List<SourceClaim>>>
	{
		private static final long serialVersionUID = -83096869601642532L;

		@SuppressWarnings("unchecked")
		@Override
		public KeyValuePair<Integer, List<SourceClaim>> apply(RheemContext context, Object... args) 
		{
			// TODO Auto-generated method stub
			//System.out.println ("Reduce Op") ;
			List<SourceClaim> merge = new ArrayList<SourceClaim>() ;
			KeyValuePair<?, ?> kv1 = (KeyValuePair<?, ?>) args[0] ;
			KeyValuePair<?, ?> kv2 = (KeyValuePair<?, ?>) args[1] ;
			//System.out.println(kv1.key) ;
			//System.out.println(kv2.key) ;
			int sum = (int)kv1.key  +  (int)kv2.key ;
			if (kv1.value != null) merge.addAll( (Collection<? extends SourceClaim>) (kv1.value )) ;
			if (kv2.value != null) merge.addAll( (Collection<? extends SourceClaim>) (kv2.value )) ;
			
			//System.out.println (sum) ;
			return new KeyValuePair<>(sum, merge) ;
		}
		
	}
	
	//raises an exception, need to be revised and corrected
	static class ReduceByKeyWrapper1 extends LogicalOperatorWrapper<KeyValuePair<Integer, KeyValuePair<Source, List<SourceClaim>>>>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = -7181035084095905908L;

		@SuppressWarnings("unchecked")
		@Override
		public KeyValuePair<Integer, KeyValuePair<Source, List<SourceClaim>>> apply(RheemContext context, Object... args) 
		{
			// TODO Auto-generated method stub
			System.out.println ("Reduce Transform") ;
			//System.out.println(args.length) ;
			KeyValuePair<?,?> kv1 = (KeyValuePair<?, ?>) args[0] ;
			//System.out.println(kv1.key) ;
			KeyValuePair<?,?> kv2 = (KeyValuePair<?, ?>) args[1] ;
			//System.out.println(kv2.key) ;
			List<SourceClaim> merge = new ArrayList<SourceClaim> () ;
			
			merge.addAll( (Collection<? extends SourceClaim>) ((KeyValuePair<?,?>)kv1.value).value ) ;
			merge.addAll( (Collection<? extends SourceClaim>) ((KeyValuePair<?,?>)kv2.value).value ) ;
			
			KeyValuePair<Source,List<SourceClaim>> kv = new KeyValuePair<Source,List<SourceClaim>>((Source) ((KeyValuePair<?,?>)kv1.value).key, merge) ;
	    
			//System.out.println(sum) ;
			return new KeyValuePair<>((Integer)kv1.key, kv) ;
		}
		
	}
	static class transformLocal extends LogicalOperatorWrapper<List<KeyValuePair<Integer, KeyValuePair<Source, List<SourceClaim>>>>>
	{

		@Override
		public List<KeyValuePair<Integer, KeyValuePair<Source, List<SourceClaim>>>> apply(RheemContext arg0, Object... args) {
			// TODO Auto-generated method stub
			System.out.println("Transform Local op") ;
			return (List<KeyValuePair<Integer, KeyValuePair<Source, List<SourceClaim>>>>) args[0] ;
		}
		
	}
	
	public Transform<?, ?> getTransformOp() {
		return transformOp;
	}
	public void setTransformOp(Transform<?, ?> transformOp) {
		this.transformOp = transformOp;
	}
	public LocalStaging getInitOp() {
		return initOp;
	}
	public void setInitOp(LocalStaging initOp) {
		this.initOp = initOp;
	}
	public Compute<?, ?> getConfidenceOp() {
		return confidenceOp;
	}
	public void setConfidenceOp(Compute<?, ?> confidenceOp) {
		this.confidenceOp = confidenceOp;
	}
	public Update<?, ?> getTrustOp() {
		return trustOp;
	}
	public void setTrustOp(Update<?, ?> trustOp) {
		this.trustOp = trustOp;
	}
	public Loop<?> getConvergenceOp() {
		return convergenceOp;
	}
	public void setConvergenceOp(Loop<?> convergenceOp) {
		this.convergenceOp = convergenceOp;
	}
	
	public double getStartTrust() {
		return startTrust;
	}
	public void setStartTrust(double startTrust) {
		this.startTrust = startTrust;
	}
	public double getStartConfidence() {
		return startConfidence;
	}
	public void setStartConfidence(double startConfidence) {
		this.startConfidence = startConfidence;
	}
	/**
	 * @return the params
	 */
	public ArrayList<Double> getParams() {
		return params;
	}
	/**
	 * @param params the params to set
	 */
	public void setParams(ArrayList<Double> params) {
		this.params = params;
	}

}
