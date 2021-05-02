/*
This script logs pipeline informations on monitoring tables:
If pipeline fails before script 7 --> @campaign_has_been_computed = False and @river_has_been_computed = False
If pipeline fails after script 7_ and before script last script --> @campaign_has_been_computed = True and @river_has_been_computed = False
If pipeline doesn't fail --> @campaign_has_been_computed = True and @river_has_been_computed = True
@startDate pipeline starts date
@finishedOn date pipeline has ends or fails
@status if pipeline fails  set hard fails otherwise SUCCESS
@reason if pipeline fails catch error raised in the pipeline
@scriptVersion pipeline version (might be pipeline version number or github commit)
@failedStep set script name where pipeline fails otherwise leave empty
*/
UPDATE bi_temp.pipelines
	SET campaign_has_been_computed = @campaign_has_been_computed,
	    river_has_been_computed = @river_has_been_computed
WHERE
	campaign_id = @campaignID;

INSERT INTO logs.bi
	(id,
	campaign_id,
	initiated_on,
	finished_on,
	status,
	reason,
	script_version,
	failed_step
	)
	VALUES

	(

	@pipelineID,
	@campaignID,
	@startDate,
	@finishedOn,
	@status,
	@reason,
	@scriptVersion,
	@failedStep

	);

update campaign.campaign
set has_been_computed = (@campaign_has_been_computed AND @river_has_been_computed)
where id in (@campaignID);