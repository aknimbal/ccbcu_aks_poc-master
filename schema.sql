/****** Object:  Table [dbo].[LoadedRecognizedItems]    Script Date: 3/13/2019 2:14:43 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[LoadedRecognizedItems](
	[SessionID] [varchar](100) NOT NULL,
	[SceneID] [varchar](100) NOT NULL,
	[ProductID] [varchar](100) NULL,
	[StoreNumber] [varchar](100) NULL,
	[SessionDate] [date] NULL,
	[SessionDateTime] [datetime2](7) NULL,
	[EmailAddress] [varchar](255) NULL,
	[TaskCode] [varchar](100) NULL,
	[TaskName] [varchar](255) NULL,
	[id] [varchar](100) NOT NULL,
	[ProductCode] [varchar](100) NULL,
	[ProductName] [varchar](255) NULL,
	[ProductType] [varchar](50) NULL,
	[CountTotal] [int] NULL,
	[CountFront] [int] NULL,
	[UpdateDateTime] [datetime2](7) NULL,
	[JobName] [varchar](50) NULL,
 CONSTRAINT [PK_LoadedRecognizedItems] PRIMARY KEY CLUSTERED 
(
	[SessionID] ASC,
	[SceneID] ASC,
	[id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_PADDING ON
GO

/****** Object:  Index [PK_LoadedRecognizedItems]    Script Date: 3/13/2019 2:16:27 PM ******/
ALTER TABLE [dbo].[LoadedRecognizedItems] ADD  CONSTRAINT [PK_LoadedRecognizedItems] PRIMARY KEY CLUSTERED 
(
	[SessionID] ASC,
	[SceneID] ASC,
	[id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON [PRIMARY]
GO





/****** Object:  Table [dbo].[TraxImages]    Script Date: 3/13/2019 2:18:22 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[TraxImages](
	[SessionID] [varchar](100) NOT NULL,
	[ImageID] [varchar](100) NOT NULL,
	[QualityIssueCode] [varchar](50) NOT NULL,
	[StoreNumber] [varchar](20) NULL,
	[SessionDate] [date] NULL,
	[SessionDateTime] [datetime2](7) NULL,
	[EmailAddress] [varchar](255) NULL,
	[TaskCode] [varchar](50) NULL,
	[TaskName] [varchar](100) NULL,
	[ImageURL] [varchar](255) NULL,
	[ImageCaptureTime] [datetime2](7) NULL,
	[QualityIssueValue] [varchar](50) NULL,
	[UpdateDateTime] [datetime2](7) NULL,
	[JobName] [varchar](100) NULL,
 CONSTRAINT [PK_TraxImages] PRIMARY KEY CLUSTERED 
(
	[SessionID] ASC,
	[ImageID] ASC,
	[QualityIssueCode] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


SET ANSI_PADDING ON
GO

/****** Object:  Index [PK_TraxImages]    Script Date: 3/13/2019 2:18:43 PM ******/
ALTER TABLE [dbo].[TraxImages] ADD  CONSTRAINT [PK_TraxImages] PRIMARY KEY CLUSTERED 
(
	[SessionID] ASC,
	[ImageID] ASC,
	[QualityIssueCode] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON [PRIMARY]
GO


