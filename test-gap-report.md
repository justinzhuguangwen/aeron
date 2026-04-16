# Aeron C 测试搬迁差距报告 (2026-04-16)

## 1. 已有 C 文件 — 缺失的 Java 用例

### ClusterBackupAgentTest → aeron_cluster_backup_agent_test (Java 3, C 20, 未匹配 3)

- `shouldLargestPositionLessThanOrEqualToInitialReplayPosition`
- `shouldReturnNullPositionIfLastTermIsNullAndSnapshotsIsEmpty`
- `shouldReturnReplayStartPositionIfAlreadyExisting`

### ClusterMemberTest → aeron_cluster_member_test (Java 24, C 52, 未匹配 20)

- `hasQuorumAtPositionReturnFalseIfNotAQuorum`
- `hasQuorumAtPositionReturnTrueIfQuorumIsAtPosition`
- `isQuorumCandidateReturnFalseWhenQuorumIsNotReached`
- `isQuorumCandidateReturnTrueWhenQuorumIsReached`
- `isQuorumLeaderReturnsFalseIfAtLeastOneNegativeVoteIsDetected`
- `isQuorumLeaderReturnsFalseWhenQuorumIsNotReached`
- `isQuorumLeaderReturnsTrueWhenQuorumIsReached`
- `isUnanimousCandidateReturnFalseIfLeaderClosesGracefully`
- `isUnanimousCandidateReturnFalseIfThereIsAMemberWithMoreUpToDateLog`
- `isUnanimousCandidateReturnFalseIfThereIsAMemberWithoutLogPosition`
- `isUnanimousCandidateReturnTrueIfTheCandidateHasTheMostUpToDateLog`
- `isUnanimousLeaderReturnsFalseIfLeaderClosesGracefully`
- `isUnanimousLeaderReturnsFalseIfNotAllNodesHadTheExpectedCandidateTermId`
- `isUnanimousLeaderReturnsFalseIfNotAllNodesVotedPositively`
- `isUnanimousLeaderReturnsFalseIfThereIsAtLeastOneNegativeVoteForAGivenCandidateTerm`
- `isUnanimousLeaderReturnsTrueIfAllNodesVotedWithTrue`
- `shouldNotVoteIfHasMoreLog`
- `shouldNotVoteIfHasNoPosition`
- `shouldReturnFalseIfLogPositionIsLessThan`
- `shouldReturnFalseIfNotActiveWhenDoingPositionChecks`

### ConsensusModuleAgentTest → aeron_consensus_module_agent_test (Java 15, C 56, 未匹配 15)

- `notifiedCommitPositionShouldNotGoBackwardsUponElectionCompletion`
- `notifiedCommitPositionShouldNotGoBackwardsUponReceivingCommitPosition`
- `notifiedCommitPositionShouldNotGoBackwardsUponReceivingNewLeadershipTerm`
- `onCommitPositionShouldUpdateTimeOfLastLeaderMessageReceived`
- `onNewLeadershipTermShouldUpdateTimeOfLastLeaderMessageReceived`
- `shouldCloseInactiveSession`
- `shouldCloseTerminatedSession`
- `shouldDelegateHandlingToRegisteredExtension`
- `shouldHandlePaddingMessageAtEndOfTerm`
- `shouldLimitActiveSessions`
- `shouldPublishLogMessageButNotSnapshotOnStandbySnapshot`
- `shouldSuspendThenResume`
- `shouldThrowClusterTerminationExceptionUponShutdown`
- `shouldThrowExceptionOnUnknownSchemaAndNoAdapter`
- `shouldUseAssignedRoleName`

### ConsensusModuleContextTest → aeron_consensus_module_context_test (Java 45, C 91, 未匹配 30)

- `clusterDirectoryNameShouldMatchClusterDirWhenClusterDirSet`
- `clusterDirectoryNameShouldMatchClusterDirWhenClusterDirectoryNameSet`
- `clusterServiceDirectoryNameShouldBeResolved`
- `clusterServiceDirectoryNameShouldBeSetFromClusterDirectoryName`
- `concludeShouldCreateLinkPointingToTheParentDirectoryOfTheMarkFile`
- `defaultAuthorisationServiceSupplierAllowsBackupAndStandby`
- `defaultTimerServiceSupplier`
- `explicitTimerServiceSupplier`
- `rejectInvalidLogChannelParameters`
- `shouldAlignMarkFileBasedOnTheMediaDriverFilePageSize`
- `shouldCreateArchiveContextUsingLocalChannelConfiguration`
- `shouldCreateElectionCounter`
- `shouldCreateLeadershipTermIdCounter`
- `shouldInstantiateAuthenticatorSupplierBasedOnTheSystemProperty`
- `shouldInstantiateAuthorisationServiceSupplierBasedOnTheSystemProperty`
- `shouldNotSetClientNameOnTheExplicitlyAssignedAeronClient`
- `shouldRejectServiceCountZeroWithoutConsensusModuleExtension`
- `shouldThrowClusterExceptionIfClockCannotBeCreated`
- `shouldThrowIfConductorInvokerModeIsNotUsed`
- `shouldUseCandidateTermIdFromClusterMarkFileIfNodeStateFileIsNew`
- `shouldUseDefaultAuthenticatorSupplierIfTheSystemPropertyIsSetToEmptyValue`
- `shouldUseDefaultAuthorisationServiceSupplierIfTheSystemPropertyIsNotSet`
- `shouldUseDefaultAuthorisationServiceSupplierIfTheSystemPropertyIsSetToEmptyValue`
- `shouldUseExplicitlyAssignArchiveContext`
- `shouldUseExplicitlyAssignedClockInstance`
- `shouldUseProvidedAAuthenticatorSupplierInstance`
- `shouldUseProvidedAuthorisationServiceSupplierInstance`
- `startupCanvassTimeoutMustCanBeSetToBeMultiplesOfTheLeaderHeartbeatTimeout`
- `unknownTimerServiceSupplier`
- `writeAuthenticatorSupplierClassNameIntoTheMarkFile`

### ConsensusModuleSnapshotTakerTest → aeron_cluster_snapshot_taker_test (Java 6, C 29, 未匹配 2)

- `snapshotPendingServiceMessageTracker`
- `snapshotPendingServiceMessageTrackerWithServiceMessagesMissedByFollower`

### ElectionTest → aeron_cluster_election_test (Java 25, C 36, 未匹配 9)

- `followerShouldProgressThroughFailedElectionsTermsImmediatelyPriorToCurrent`
- `followerShouldProgressThroughInterimElectionsTerms`
- `followerShouldReplayAndCatchupWhenLateJoiningClusterInSameTerm`
- `followerShouldReplicateAndSendAppendPositionWhenLogReplicationDone`
- `followerShouldReplicateLogBeforeReplayDuringElection`
- `followerShouldReplicateReplayAndCatchupWhenLateJoiningClusterInLaterTerm`
- `followerShouldTimeoutLeaderIfReplicateLogPositionIsNotCommittedByLeader`
- `followerShouldUseInitialLeadershipTermIdAndInitialTermBaseLogPositionWhenRecordingLogIsEmpty`
- `leaderShouldMoveToLogReplicationThenWaitForCommitPosition`

### PendingServiceMessageTrackerTest → aeron_cluster_pending_message_tracker_test (Java 7, C 7, 未匹配 7)

- `loadInvalid`
- `loadValid`
- `snapshotAfterEnqueueAndPollBeforeSweep`
- `snapshotAfterEnqueueBeforePollAndSweep`
- `snapshotAfterEnqueuePollAndSweepForFollower`
- `snapshotAfterEnqueuePollAndSweepForLeader`
- `snapshotEmpty`

### PriorityHeapTimerServiceTest → aeron_cluster_timer_service_test (Java 25, C 24, 未匹配 18)

- `cancelTimerByCorrelationIdAfterPoll`
- `cancelTimerByCorrelationIdIsANoOpIfNoTimersRegistered`
- `cancelTimerByCorrelationIdReturnsFalseForUnknownCorrelationId`
- `cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheLastTimer`
- `cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheTimer`
- `expireThanCancelTimer`
- `moveDownAnExistingTimerAndCancelAnotherOne`
- `moveUpAnExistingTimerAndCancelAnotherOne`
- `pollIsANoOpWhenNoTimersWhereScheduled`
- `pollShouldNotExpireTimerIfHandlerReturnsFalse`
- `pollShouldRemovedExpiredTimers`
- `scheduleTimerForAnExistingCorrelationIdIsANoOpIfDeadlineDoesNotChange`
- `scheduleTimerForAnExistingCorrelationIdShouldShiftEntryDownWhenDeadlineIsIncreasing`
- `scheduleTimerForAnExistingCorrelationIdShouldShiftEntryUpWhenDeadlineIsDecreasing`
- `shouldReuseCanceledTimerEntriesFromAFreeList`
- `shouldReuseExpiredEntriesFromAFreeList`
- `snapshotProcessesAllScheduledTimers`
- `throwsNullPointerExceptionIfTimerHandlerIsNull`

### RecordingLogTest → aeron_cluster_recording_log_test (Java 33, C 56, 未匹配 15)

- `appendTermShouldOnlyAllowASingleValidTermForTheSameLeadershipTermId`
- `entriesInTheRecordingLogShouldBeSorted`
- `entryToString`
- `shouldAppendSnapshotWithLeadershipTermIdOutOfOrder`
- `shouldBackFillPriorTerm`
- `shouldCorrectlyOrderSnapshots`
- `shouldFailToRecoverSnapshotsMarkedInvalidIfFieldsDoNotMatchCorrectly`
- `shouldFindSnapshotAtOrBeforeOrLowest`
- `shouldGetLatestStandbySnapshotsGroupedByEndpoint`
- `shouldHandleEntriesStraddlingPageBoundary`
- `shouldInsertStandbySnapshotInRecordingLog`
- `shouldInvalidateLatestAnySnapshots`
- `shouldNotIncludeStandbySnapshotInRecoveryPlan`
- `shouldRejectSnapshotEntryIfEndpointIsTooLong`
- `shouldThrowIfLastTermIsUnfinishedAndTermBaseLogPositionIsNotSpecified`

### SessionManagerTest → aeron_cluster_session_manager_test (Java 2, C 25, 未匹配 2)

- `shouldProcessPendingStandbySnapshotNotificationsAfterProcessingDelay`
- `shouldProcessPendingStandbySnapshotNotificationsAfterReachingCommitPosition`

**已有文件缺失用例小计: 121**

## 2. 完全缺失的 C 测试文件

### 2.1 Cluster 单元测试

| Java 文件 | 用例数 | 用例列表 |
|-----------|------:|---------|
| AeronClusterAsyncConnectTest | 7 | `initialState`, `shouldCloseAsyncSubscription`, `shouldCloseEgressSubscription`, `shouldCloseAsyncPublication`, `shouldCloseIngressPublication`, ... (+2) |
| AeronClusterContextTest | 2 | `concludeThrowsConfigurationExceptionIfIngressChannelIsSetToIpcAndIngressEndpointsSpecified`, `clientNameMustNotExceedMaxLength` |
| AeronClusterTest | 2 | `shouldCloseIngressPublicationWhenEgressImageCloses`, `shouldCloseItselfAfterReachingMaxPositionOnTheIngressPublication` |
| AuthenticationTest | 5 | `shouldAuthenticateOnConnectRequestWithEmptyCredentials`, `shouldAuthenticateOnConnectRequestWithCredentials`, `shouldAuthenticateOnChallengeResponse`, `shouldRejectOnConnectRequest`, `shouldRejectOnChallengeResponse` |
| ClusterBackupContextTest | 7 | `throwsIllegalStateExceptionIfThereIsAnActiveMarkFile`, `clusterDirectoryNameShouldMatchClusterDirWhenClusterDirSet`, `clusterDirectoryNameShouldMatchClusterDirWhenClusterDirectoryNameSet`, `concludeShouldCreateMarkFileDirSetViaSystemProperty`, `concludeShouldCreateMarkFileDirSetDirectly`, ... (+2) |
| ClusterMarkFileTest | 13 | `shouldCallForceIfMarkFileIsNotClosed`, `shouldNotCallForceIfMarkFileIsClosed`, `shouldUpdateExistingMarkFile`, `shouldUnmapBufferUponClose`, `clusterIdAccessors`, ... (+8) |
| ClusterNodeRestartTest | 10 | `shouldRestartServiceWithReplay`, `shouldRestartServiceWithReplayAndContinue`, `shouldRestartServiceFromEmptySnapshot`, `shouldRestartServiceFromSnapshot`, `shouldRestartServiceFromSnapshotWithFurtherLog`, ... (+5) |
| ClusterNodeTest | 5 | `shouldConnectAndSendKeepAlive`, `shouldEchoMessageViaServiceUsingDirectOffer`, `shouldEchoMessageViaServiceUsingTryClaim`, `shouldScheduleEventInService`, `shouldSendResponseAfterServiceMessage` |
| ClusterTimerTest | 3 | `shouldRestartServiceWithTimerFromSnapshotWithFurtherLog`, `shouldTriggerRescheduledTimerAfterReplay`, `shouldRescheduleTimerWhenSchedulingWithExistingCorrelationId` |
| ClusterWithNoServicesTest | 4 | `shouldConnectAndSendKeepAliveWithExtensionLoaded`, `shouldSnapshotExtensionState`, `shouldShutdownWithExtension`, `shouldAbortWithExtension` |
| ClusteredServiceAgentTest | 3 | `shouldClaimAndWriteToBufferWhenFollower`, `shouldAbortClusteredServiceIfCommitPositionCounterIsClosed`, `shouldLogErrorInsteadOfThrowingIfSessionIsNotFoundOnClose` |
| ClusteredServiceContainerContextTest | 11 | `throwsIllegalStateExceptionIfAnActiveMarkFileExists`, `concludeShouldCreateMarkFileDirSetViaSystemProperty`, `concludeShouldCreateMarkFileDirSetDirectly`, `concludeShouldCreateMarkFileLinkInTheParentDirectoryOfTheClusterMarkFile`, `shouldInitializeClusterDirectoryNameFromTheAssignedClusterDir`, ... (+6) |
| ConsensusModuleConfigurationTest | 2 | `shouldUseDenyAllAuthorisationSupplierWhenPropertySet`, `shouldUseAllowAllAuthorisationSupplierWhenPropertySet` |
| EgressAdapterTest | 10 | `onFragmentShouldDelegateToEgressListenerOnUnknownSchemaId`, `defaultEgressListenerBehaviourShouldThrowClusterExceptionOnUnknownSchemaId`, `onFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches`, `onFragmentIsANoOpIfSessionIdDoesNotMatchOnSessionMessage`, `onFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches`, ... (+5) |
| EgressPollerTest | 2 | `shouldIgnoreUnknownMessageSchema`, `shouldHandleSessionMessage` |
| LogSourceValidatorTest | 3 | `leaderLogSourceTypeShouldOnlyAcceptLeader`, `followerLogSourceTypeShouldOnlyAcceptFollowerAndUnknown`, `anyLogSourceTypeShouldAny` |
| NameResolutionClusterNodeTest | 1 | `shouldConnectAndSendKeepAliveWithBadName` |
| NodeStateFileTest | 7 | `shouldFailIfCreateNewFalseAndFileDoesNotExist`, `shouldCreateIfCreateNewTrueAndFileDoesNotExist`, `shouldHaveNullCandidateTermIdOnInitialCreation`, `shouldPersistCandidateTermId`, `shouldThrowIfVersionMismatch`, ... (+2) |
| PublicationGroupTest | 8 | `shouldUseAllPublicationsInListWhenGettingNextPublication`, `shouldNextPublicationAndReturnSameWithCurrent`, `shouldResultNullWhenCurrentCalledWithoutNext`, `shouldCloseWhenCurrentIsNotNullWhenNextIsCalled`, `shouldExcludeCurrentPublication`, ... (+3) |
| RecordingLogValidatorTest | 1 | `shouldInvalidateSnapshotsWithUnknownRecordings` |
| RecordingReplicationTest | 4 | `shouldIndicateAppropriateStatesAsSignalsAreReceived`, `shouldNotStopReplicationIfStopSignalled`, `shouldFailIfRecordingLogIsDeletedDuringReplication`, `shouldPollForProgressAndFailIfNotProgressing` |
| ServiceSnapshotTakerTest | 2 | `snapshotSessionUsesTryClaimIfDataFitIntoMaxPayloadSize`, `snapshotSessionUsesOfferIfDataDoesNotIntoMaxPayloadSize` |
| SessionEventCodecCompatibilityTest | 1 | `readingVersion12UsingVersion5Codec` |
| SnapshotReplicationTest | 3 | `shouldReplicateTwoSnapshots`, `shouldNotBeCompleteIfNotSynced`, `closeWillCloseUnderlyingSnapshotReplication` |
| StandbySnapshotReplicatorTest | 8 | `shouldReplicateStandbySnapshots`, `shouldPassSignalsToRecordingReplication`, `shouldHandleNoStandbySnapshots`, `shouldSwitchEndpointsOnMultipleReplicationException`, `shouldSwitchEndpointsOnArchivePollForSignalsException`, ... (+3) |

**缺失文件用例小计: 124**

### 2.2 Cluster 系统测试

| Java 文件 | 用例数 |
|-----------|------:|
| AppointedLeaderTest | 2 |
| ClusterBackupTest | 17 |
| ClusterNetworkPartitionTest | 5 |
| ClusterNetworkTopologyTest | 3 |
| ClusterSessionReliabilityTest | 5 |
| ClusterTest | 81 |
| ClusterToolTest | 16 |
| ClusterUncommittedStateTest | 1 |
| FailedFirstElectionClusterTest | 1 |
| InitiateShutdownThenImmediatelyCloseLeaderTest | 1 |
| MultiClusteredServicesTest | 1 |
| MultiModuleSharedDriverTest | 2 |
| MultiNodeTest | 4 |
| OffsetMillisecondClusterClockTest | 1 |
| RacingCatchupClusterTest | 1 |
| RecoverAfterFailedCatchupClusterTest | 1 |
| ServiceIpcIngressMessageTest | 7 |
| SingleNodeTest | 5 |
| StalledLeaderLogReplicationClusterTest | 1 |
| StartFromTruncatedRecordingLogTest | 1 |
| TestClusterTest | 1 |

**系统测试缺失小计: 157**

## 3. 总结

| 类别 | 缺失用例 |
|------|--------:|
| 已有文件未匹配的用例 | 121 |
| 完全缺失的单元测试文件 | 124 |
| 完全缺失的系统测试文件 | 157 |
| **合计** | **402** |