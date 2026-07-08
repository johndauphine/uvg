DROP INDEX [IX_Posts_OwnerUserId] ON dbo.Posts;
GO

ALTER TABLE dbo.Users DROP COLUMN [LastSeenAt];
GO

DROP TABLE dbo.PostModerationEvents;
GO
