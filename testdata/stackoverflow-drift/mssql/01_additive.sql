ALTER TABLE dbo.Users ADD [LastSeenAt] DATETIME2 NULL;
GO

ALTER TABLE dbo.Users ADD [ProfileScore] DECIMAL(10, 2) NOT NULL
    CONSTRAINT [DF_Users_ProfileScore] DEFAULT ((0.00));
GO

CREATE INDEX [IX_Posts_OwnerUserId] ON dbo.Posts([OwnerUserId]);
GO

CREATE TABLE dbo.PostModerationEvents (
    [Id] INT IDENTITY(1, 1) NOT NULL,
    [PostId] INT NOT NULL,
    [ModeratorUserId] INT NULL,
    [EventType] NVARCHAR(40) NOT NULL,
    [Reason] NVARCHAR(400) NULL,
    [CreatedAt] DATETIME2 NOT NULL
        CONSTRAINT [DF_PostModerationEvents_CreatedAt] DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT [PK_PostModerationEvents_Id] PRIMARY KEY ([Id]),
    CONSTRAINT [FK_PostModerationEvents_Posts] FOREIGN KEY ([PostId]) REFERENCES dbo.Posts([Id]),
    CONSTRAINT [FK_PostModerationEvents_Users] FOREIGN KEY ([ModeratorUserId]) REFERENCES dbo.Users([Id]),
    CONSTRAINT [CK_PostModerationEvents_EventType] CHECK ([EventType] IN (N'flagged', N'closed', N'reopened'))
);
GO

CREATE INDEX [IX_PostModerationEvents_PostId] ON dbo.PostModerationEvents([PostId]);
GO
