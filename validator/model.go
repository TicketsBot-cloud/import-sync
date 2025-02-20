package validator

import (
	"time"

	"github.com/TicketsBot-cloud/database"
)

type TicketUnion[T any] struct {
	TicketId int `json:"ticket_id"`
	Data     T   `json:"data"`
}

type GuildData struct {
	GuildId                    uint64                                    `json:"guild_id,string"`
	ActiveLanguage             *string                                   `json:"active_language"`
	ArchiveChannel             *uint64                                   `json:"archive_channel,string"`
	ArchiveMessages            []TicketUnion[database.ArchiveMessage]    `json:"archive_messages"`
	AutocloseSettings          *database.AutoCloseSettings               `json:"autoclose_settings"`
	AutocloseExcluded          []int                                     `json:"autoclose_excluded"` // ticket IDs
	GuildBlacklistedUsers      []uint64                                  `json:"guild_blacklisted_users"`
	ChannelCategory            *uint64                                   `json:"channel_category,string"`
	ClaimSettings              *database.ClaimSettings                   `json:"claim_settings"`
	CloseConfirmationEnabled   bool                                      `json:"close_confirmation_enabled"`
	CloseReasons               []TicketUnion[database.CloseMetadata]     `json:"close_reasons"`
	CustomColors               map[int16]int                             `json:"custom_colors"`
	EmbedFields                []database.EmbedField                     `json:"embed_fields"`
	Embeds                     []database.CustomEmbed                    `json:"embeds"`
	ExitSurveyResponses        []TicketUnion[ExitSurveyResponse]         `json:"exit_survey_responses"`
	FeedbackEnabled            bool                                      `json:"feedback_enabled"`
	FirstResponseTimes         []FirstResponseTime                       `json:"first_response_times"`
	FormInputs                 []database.FormInput                      `json:"form_inputs"`
	Forms                      []database.Form                           `json:"forms"`
	GuildIsGloballyBlacklisted bool                                      `json:"guild_is_globally_blacklisted"`
	GuildMetadata              database.GuildMetadata                    `json:"guild_metadata"`
	MultiPanels                []database.MultiPanel                     `json:"multi_panels"`
	MultiPanelTargets          map[int][]int                             `json:"multi_panel_targets"` // multi_panel_id -> [panel_ids]
	NamingScheme               *database.NamingScheme                    `json:"naming_scheme"`
	OnCallUsers                []uint64                                  `json:"on_call_users"`
	PanelAccessControlRules    map[int][]database.PanelAccessControlRule `json:"panel_access_control_rules"` // panel_id -> rules
	PanelMentionUser           map[int]bool                              `json:"panel_mention_user"`
	PanelRoleMentions          map[int][]uint64                          `json:"panel_role_mentions"`
	Panels                     []database.Panel                          `json:"panels"`
	PanelTeams                 map[int][]int                             `json:"panel_teams"`  // panel_id -> [team_ids]
	Participants               map[int][]uint64                          `json:"participants"` // ticket_id -> [user_ids]
	UserPermissions            []Permission                              `json:"user_permissions"`
	GuildBlacklistedRoles      []uint64                                  `json:"guild_blacklisted_roles"`
	RolePermissions            []Permission                              `json:"role_permissions"`
	ServiceRatings             []TicketUnion[int16]                      `json:"service_ratings"`
	Settings                   database.Settings                         `json:"settings"`
	SupportTeamUsers           map[int][]uint64                          `json:"support_team_users"` // team_id -> [user_ids]
	SupportTeamRoles           map[int][]uint64                          `json:"support_team_roles"` // team_id -> [role_ids]
	SupportTeams               []database.SupportTeam                    `json:"support_teams"`
	Tags                       []database.Tag                            `json:"tags"`
	TicketClaims               []TicketUnion[uint64]                     `json:"ticket_claims"`
	TicketLastMessages         []TicketUnion[database.TicketLastMessage] `json:"ticket_last_messages"`
	TicketLimit                *int                                      `json:"ticket_limit"`
	TicketAdditionalMembers    map[int][]uint64                          `json:"ticket_additional_members"` // ticket_id -> [user_ids]
	TicketPermissions          database.TicketPermissions                `json:"ticket_permissions"`
	Tickets                    []database.Ticket                         `json:"tickets"`
	UsersCanClose              bool                                      `json:"users_can_close"`
	WelcomeMessage             *string                                   `json:"welcome_message"`
}

// Shims

type FirstResponseTime struct {
	TicketId     int           `json:"ticket_id"`
	UserId       uint64        `json:"user_id,string"`
	ResponseTime time.Duration `json:"response_time"`
}

type Permission struct {
	Snowflake uint64 `json:"snowflake,string"`
	IsSupport bool   `json:"is_support"`
	IsAdmin   bool   `json:"is_admin"`
}

type ExitSurveyResponse struct {
	FormId     *int    `json:"form_id"`
	QuestionId *int    `json:"question_id"`
	Response   *string `json:"response"`
}
