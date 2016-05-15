package org.qcri.truthdiscovery.util.voter;

import org.qcri.truthdiscovery.util.dataModel.data.DataSet;
import org.qcri.truthdiscovery.util.dataModel.quality.dataQuality.ConvergenceTester;
import org.qcri.truthdiscovery.util.dataModel.quality.voterResults.NormalVoterQualityMeasures;
import org.qcri.truthdiscovery.util.dataModel.quality.voterResults.VoterQualityMeasures;
import org.qcri.truthdiscovery.util.experiment.profiling.Profiler;

/**
 * Super class for all Voters
 * @author dalia, Laure
 *
 */
public abstract class Voter {

	protected DataSet dataSet;
	protected VoterQualityMeasures voterQuality;
	protected boolean singlePropertyValue; 
	protected boolean onlyMaxValueIsTrue;
	
	protected double startingTrust = 0.8;
	protected VoterParameters params;
	
	public Voter(DataSet dataSet, VoterParameters params) {
		this.dataSet = dataSet;
		this.dataSet.resetDataSet(params.getStartingTrust(), params.getStartingConfidence(), params.getStartingErrorFactor());

		this.startingTrust = params.getStartingTrust();
		ConvergenceTester.convergenceThreshold = params.getCosineSimDiffStoppingCriteria();
		this.params = params;
		initParameters();
	}
	/**
	 * 
	 */
	protected abstract void initParameters();
	/**
	 * 
	 * @param singlePropertyValue : list data type is provided as single value over multiple claims, or one claim with multiple values
	 * @param onlyMaxValueIsTrue
	 * @return
	 */
	public VoterQualityMeasures launchVoter(boolean convergence100, boolean profileMemory) {
		this.dataSet.resetDataSet(params.getStartingTrust(), params.getStartingConfidence(), params.getStartingErrorFactor());
		voterQuality = new NormalVoterQualityMeasures(dataSet);
		Profiler profiler = null;
		if (profileMemory) {
			profiler = new Profiler();
			Thread profilerThread = new Thread(profiler);
			profilerThread.start();
		}
		voterQuality.getTimings().startVoterDuration();
		int iterationCount = runVoter(convergence100);
		voterQuality.getTimings().endVoterDuration();
		if (profileMemory) {
			profiler.stopProfiling(voterQuality);
//			System.out.println(voterQuality.getMaxMemoryConsumption());
		}

		voterQuality.computeTruth(onlyMaxValueIsTrue);

		voterQuality.setNumberOfIterations(iterationCount);
		voterQuality.computeVoterQualityMeasures(singlePropertyValue); // list data type is provided as single value over multiple claims

		return voterQuality;
	}

	public void computeMeasuresPerIteration(boolean resetTrueValues, double trustCosineSimDiff, double confCosineSimDiff){
		if (resetTrueValues) {
			voterQuality.computeTruth(onlyMaxValueIsTrue);
		}
		voterQuality.computeVoterQualityMeasures(singlePropertyValue);
		voterQuality.getPrecisionPerIteration().add(voterQuality.getPrecision());
		voterQuality.getAccuracyPerIteration().add(voterQuality.getAccuracy());
		voterQuality.getRecallPerIteration().add(voterQuality.getRecall());
		voterQuality.getSpecificityPerIteration().add(voterQuality.getSpecificity());
		voterQuality.getTruePositivePercentagePerIteration().add(voterQuality.getTruePositivePercentage());

		voterQuality.getTruePositivePerIteration().add(voterQuality.getTruePositive());
		voterQuality.getTrueNegativePerIteration().add(voterQuality.getTrueNegative());
		voterQuality.getFalsePositivePerIteration().add(voterQuality.getFalsePositive());
		voterQuality.getFalseNegativePerIteration().add(voterQuality.getFalseNegative());

		voterQuality.getIterationEndingTime().add(new Double(System.currentTimeMillis()) - voterQuality.getTimings().getStartingTime());

		voterQuality.getTrustCosineSimilarityPerIteration().add(trustCosineSimDiff);
		voterQuality.getConfCosineSimilarityPerIteration().add(confCosineSimDiff);
		
	}
	protected abstract int runVoter(boolean convergence100);

	public DataSet getDataSet() {
		return dataSet;
	}
}
