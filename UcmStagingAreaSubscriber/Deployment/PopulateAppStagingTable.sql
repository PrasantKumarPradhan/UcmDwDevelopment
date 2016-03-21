

DECLARE  @AppStagingTable TABLE(StagingQueueId INT,StagingTable VARCHAR(100),TargetTable VARCHAR(100),MergeKeyColumnList VARCHAR(400),DeDupe BIT,DedupeKeyColumn VARCHAR(100),TruncateOnCompletion BIT)

INSERT INTO @AppStagingTable (StagingQueueId,StagingTable,TargetTable,MergeKeyColumnList,DeDupe,DedupeKeyColumn,TruncateOnCompletion)

select 1, 'AccountQueue', 'Account', 'AccountId', 1, 'ArchiveDtim', 1 UNION
select 2, 'AgencyCustomerAccountRelationQueue', 'AgencyCustomerAccountRelation', 'AgencyCustomerAccountRelationId', 1, 'ArchiveDtim', 1 UNION
select 3, 'CustomerQueue', 'Customer', 'CustomerId', 1, 'ArchivedDtim', 1 UNION
select 4, 'AccountAccountRelationQueue', 'AccountAccountRelation', 'AccountId,RelationShipID', 1, 'ArchivedDtim', 1 UNION
select 5, 'AccountPaymentInstrQueue', 'AccountPaymentInstr', 'PaymentInstrId,AccountId', 1, 'ArchiveDTim', 1 UNION
select 6, 'AccountRelationQueue', 'AccountRelation', 'RelationShipID', 1, 'ArchiveDTim', 1 UNION
select 7, 'CouponQueue', 'Coupon', 'CouponId', 1, 'ArchiveDTim', 1 UNION
select 8, 'CouponBatchQueue', 'CouponBatch', 'CouponBatchId', 1, 'ArchiveDTim', 1 UNION
select 9, 'CouponClassQueue', 'CouponClass', 'CouponClassId', 1, 'ArchiveDTim', 1 UNION
select 10, 'CouponClassMediumQueue', 'CouponClassMedium', 'CouponClassId,MediumId', 1, 'ArchiveDTim', 1 UNION
select 11, 'CouponMarketingChannelQueue', 'CouponMarketingChannel', 'CouponMarketingChannelId', 1, 'ArchiveDTim', 1 UNION
select 12, 'CouponPurposeQueue', 'CouponPurpose', 'CouponPurposeId', 1, 'ArchiveDTim', 1 UNION
select 13, 'CouponRedemptionQueue', 'CouponRedemption', 'CouponRedemptionId', 1, 'ArchiveDTim', 1 UNION
select 14, 'CurrencyQueue', 'Currency', 'CurrencyID', 1, 'ArchiveDTim', 1 UNION
select 15, 'LanguageQueue', 'Language', 'LanguageID', 1, 'ArchiveDTim', 1 UNION
select 16, 'LanguageLocaleQueue', 'LanguageLocale', 'LanguageLocaleID', 1, 'ArchiveDTim', 1 UNION
select 17, 'PaymentInstrQueue', 'PaymentInstr', 'PaymentInstrId', 1, 'ArchiveDTim', 1 UNION
select 18, 'PubAccountQueue', 'PubAccount', 'AccountId', 1, 'ArchiveDTim', 1 UNION
select 19, 'PartnerCustomerQueue', 'PartnerCustomer', 'PartnerCustomerId', 1, 'ArchiveDTim', 1 UNION
select 20, 'PartnerHierarchyQueue', 'PartnerHierarchy', 'CustomerId,PartnerTypeId', 1, 'ArchiveDTim', 1 UNION
select 21, 'CountryQueue', 'Country', 'CountryID', 1, 'ArchiveDTim', 1 UNION
select 22, 'FraudReasonCodeQueue', 'FraudReasonCode', 'FraudReasonCodeId', 1, 'ArchivedDTim', 1 UNION
select 23, 'DistributionChannelQueue', 'DistributionChannel', 'DistributionChannelId', 1, 'ArchiveDTim', 1 UNION
select 24, 'AdTypeQueue', 'AdType', 'AdTypeId', 1, 'ArchiveDTim', 1 


   


INSERT INTO dbo.AppStagingTable 
(
	StagingQueueId,
	StagingTable,
	TargetTable,
	MergeKeyColumnList,
	DeDupe,
	DedupeKeyColumn,
	TruncateOnCompletion
)
SELECT 
	t.StagingQueueId,
	t.StagingTable,
	t.TargetTable,
	t.MergeKeyColumnList,
	t.DeDupe,
	t.DedupeKeyColumn,
	t.TruncateOnCompletion
FROM @AppStagingTable t LEFT OUTER JOIN  dbo.AppStagingTable m ON t.StagingQueueId = m.StagingQueueId
WHERE m.StagingQueueId IS NULL

UPDATE m SET 
	m.StagingTable = t.StagingTable,
	m.TargetTable = t.TargetTable,
	m.MergeKeyColumnList = t.MergeKeyColumnList,
	m.DeDupe = t.DeDupe,
	m.DedupeKeyColumn = t.DedupeKeyColumn,
	m.TruncateOnCompletion = t.TruncateOnCompletion
FROM @AppStagingTable t INNER JOIN dbo.AppStagingTable m ON t.StagingQueueId = m.StagingQueueId
WHERE 
	m.StagingTable <> t.StagingTable OR
	m.TargetTable <> t.TargetTable OR
	m.MergeKeyColumnList <> t.MergeKeyColumnList OR
	m.DeDupe <> t.DeDupe OR
	ISNULL(m.DedupeKeyColumn,'') <> ISNULL(t.DedupeKeyColumn,'') OR
	m.TruncateOnCompletion <> t.TruncateOnCompletion
