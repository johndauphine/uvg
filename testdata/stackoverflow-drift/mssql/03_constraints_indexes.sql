ALTER TABLE dbo.Users ADD CONSTRAINT [CK_Users_ProfileScore_NonNegative]
    CHECK ([ProfileScore] >= 0);
GO

ALTER TABLE dbo.PostTypes ADD CONSTRAINT [UQ_PostTypes_Type] UNIQUE ([Type]);
GO

ALTER TABLE dbo.Posts ADD CONSTRAINT [CK_Posts_PostTypeId_Known]
    CHECK ([PostTypeId] IN (1, 2, 3, 4, 5, 6, 7, 8));
GO

ALTER TABLE dbo.PostModerationEvents ADD CONSTRAINT [UQ_PostModerationEvents_Post_Event_CreatedAt]
    UNIQUE ([PostId], [EventType], [CreatedAt]);
GO

CREATE INDEX [IX_Comments_UserId_CreationDate]
    ON dbo.Comments([UserId], [CreationDate]);
GO
