package vault

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/vault/helper/timeutil"
	"github.com/hashicorp/vault/sdk/framework"
	"github.com/hashicorp/vault/sdk/logical"
)

// activityQueryPath is available in every namespace
func (b *SystemBackend) activityQueryPath() *framework.Path {
	return &framework.Path{
		Pattern: "internal/counters/activity$",
		Fields: map[string]*framework.FieldSchema{
			"start_time": {
				Type:        framework.TypeTime,
				Description: "Start of query interval",
			},
			"end_time": {
				Type:        framework.TypeTime,
				Description: "End of query interval",
			},
			"limit_namespaces": {
				Type:        framework.TypeInt,
				Default:     0,
				Description: "Limit query output by namespaces",
			},
		},
		HelpSynopsis:    strings.TrimSpace(sysHelp["activity-query"][0]),
		HelpDescription: strings.TrimSpace(sysHelp["activity-query"][1]),

		Operations: map[logical.Operation]framework.OperationHandler{
			logical.ReadOperation: &framework.PathOperation{
				Callback: b.handleClientMetricQuery,
				Summary:  "Report the client count metrics, for this namespace and all child namespaces.",
			},
		},
	}
}

// monthlyActivityCountPath is available in every namespace
func (b *SystemBackend) monthlyActivityCountPath() *framework.Path {
	return &framework.Path{
		Pattern:         "internal/counters/activity/monthly$",
		HelpSynopsis:    strings.TrimSpace(sysHelp["activity-monthly"][0]),
		HelpDescription: strings.TrimSpace(sysHelp["activity-monthly"][1]),
		Operations: map[logical.Operation]framework.OperationHandler{
			logical.ReadOperation: &framework.PathOperation{
				Callback: b.handleMonthlyActivityCount,
				Summary:  "Report the number of clients for this month, for this namespace and all child namespaces.",
			},
		},
	}
}

func (b *SystemBackend) activityPaths() []*framework.Path {
	return []*framework.Path{
		b.monthlyActivityCountPath(),
		b.activityQueryPath(),
	}
}

// rootActivityPaths are available only in the root namespace
func (b *SystemBackend) rootActivityPaths() []*framework.Path {
	return []*framework.Path{
		b.activityQueryPath(),
		b.monthlyActivityCountPath(),
		{
			Pattern: "internal/counters/config$",
			Fields: map[string]*framework.FieldSchema{
				"default_report_months": {
					Type:        framework.TypeInt,
					Default:     12,
					Description: "Number of months to report if no start date specified.",
				},
				"retention_months": {
					Type:        framework.TypeInt,
					Default:     24,
					Description: "Number of months of client data to retain. Setting to 0 will clear all existing data.",
				},
				"enabled": {
					Type:        framework.TypeString,
					Default:     "default",
					Description: "Enable or disable collection of client count: enable, disable, or default.",
				},
			},
			HelpSynopsis:    strings.TrimSpace(sysHelp["activity-config"][0]),
			HelpDescription: strings.TrimSpace(sysHelp["activity-config"][1]),
			Operations: map[logical.Operation]framework.OperationHandler{
				logical.ReadOperation: &framework.PathOperation{
					Callback: b.handleActivityConfigRead,
					Summary:  "Read the client count tracking configuration.",
				},
				logical.UpdateOperation: &framework.PathOperation{
					Callback: b.handleActivityConfigUpdate,
					Summary:  "Enable or disable collection of client count, set retention period, or set default reporting period.",
				},
			},
		},
		{
			Pattern: "internal/counters/activity/export$",
			Fields: map[string]*framework.FieldSchema{
				"start_time": {
					Type:        framework.TypeTime,
					Description: "Start of query interval",
				},
				"end_time": {
					Type:        framework.TypeTime,
					Description: "End of query interval",
				},
				"format": {
					Type:        framework.TypeString,
					Description: "Format of the file. Either a CSV or a JSON file with an object per line.",
					Default:     "json",
				},
			},
			HelpSynopsis:    strings.TrimSpace(sysHelp["activity-export"][0]),
			HelpDescription: strings.TrimSpace(sysHelp["activity-export"][1]),

			Operations: map[logical.Operation]framework.OperationHandler{
				logical.ReadOperation: &framework.PathOperation{
					Callback: b.handleClientExport,
					Summary:  "Report the client count metrics, for this namespace and all child namespaces.",
				},
			},
		},
	}
}

func parseStartEndTimes(a *ActivityLog, d *framework.FieldData) (time.Time, time.Time, error) {
	startTime := d.Get("start_time").(time.Time)
	endTime := d.Get("end_time").(time.Time)

	// If a specific endTime is used, then respect that
	// otherwise we want to give the latest N months, so go back to the start
	// of the previous month
	//
	// Also convert any user inputs to UTC to avoid
	// problems later.
	if endTime.IsZero() {
		endTime = timeutil.EndOfMonth(timeutil.StartOfPreviousMonth(time.Now().UTC()))
	} else {
		endTime = endTime.UTC()
	}
	if startTime.IsZero() {
		startTime = a.DefaultStartTime(endTime)
	} else {
		startTime = startTime.UTC()
	}
	if startTime.After(endTime) {
		return time.Time{}, time.Time{}, fmt.Errorf("start_time is later than end_time")
	}

	return startTime, endTime, nil
}

// This endpoint is not used by the UI. The UI's "export" feature is entirely client-side.
func (b *SystemBackend) handleClientExport(ctx context.Context, req *logical.Request, d *framework.FieldData) (*logical.Response, error) {
	a := b.Core.activityLog
	if a == nil {
		return logical.ErrorResponse("no activity log present"), nil
	}

	startTime, endTime, err := parseStartEndTimes(a, d)
	if err != nil {
		return logical.ErrorResponse(err.Error()), nil
	}

	// This is to avoid the default 90s context timeout.
	timeout := 10 * time.Minute
	if durationRaw := os.Getenv("VAULT_ACTIVITY_EXPORT_DURATION"); durationRaw != "" {
		d, err := time.ParseDuration(durationRaw)
		if err == nil {
			timeout = d
		}
	}

	runCtx, cancelFunc := context.WithTimeout(b.Core.activeContext, timeout)
	defer cancelFunc()

	err = a.writeExport(runCtx, req.ResponseWriter, d.Get("format").(string), startTime, endTime)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (b *SystemBackend) handleClientMetricQuery(ctx context.Context, req *logical.Request, d *framework.FieldData) (*logical.Response, error) {
	a := b.Core.activityLog
	if a == nil {
		return logical.ErrorResponse("no activity log present"), nil
	}

	startTime, endTime, err := parseStartEndTimes(a, d)
	if err != nil {
		return logical.ErrorResponse(err.Error()), nil
	}

	var limitNamespaces int
	if limitNamespacesRaw, ok := d.GetOk("limit_namespaces"); ok {
		limitNamespaces = limitNamespacesRaw.(int)
	}

	results, err := a.handleQuery(ctx, startTime, endTime, limitNamespaces)
	if err != nil {
		return nil, err
	}
	if results == nil {
		resp204, err := logical.RespondWithStatusCode(nil, req, http.StatusNoContent)
		return resp204, err
	}

	return &logical.Response{
		Data: results,
	}, nil
}

func (b *SystemBackend) handleMonthlyActivityCount(ctx context.Context, req *logical.Request, d *framework.FieldData) (*logical.Response, error) {
	a := b.Core.activityLog
	if a == nil {
		return logical.ErrorResponse("no activity log present"), nil
	}

	results, err := a.partialMonthClientCount(ctx)
	if err != nil {
		return nil, err
	}
	if results == nil {
		return logical.RespondWithStatusCode(nil, req, http.StatusNoContent)
	}

	return &logical.Response{
		Data: results,
	}, nil
}

func (b *SystemBackend) handleActivityConfigRead(ctx context.Context, req *logical.Request, d *framework.FieldData) (*logical.Response, error) {
	a := b.Core.activityLog
	if a == nil {
		return logical.ErrorResponse("no activity log present"), nil
	}

	config, err := a.loadConfigOrDefault(ctx)
	if err != nil {
		return nil, err
	}

	qa, err := a.queriesAvailable(ctx)
	if err != nil {
		return nil, err
	}

	if config.Enabled == "default" {
		config.Enabled = activityLogEnabledDefaultValue
	}

	return &logical.Response{
		Data: map[string]interface{}{
			"default_report_months": config.DefaultReportMonths,
			"retention_months":      config.RetentionMonths,
			"enabled":               config.Enabled,
			"queries_available":     qa,
		},
	}, nil
}

func (b *SystemBackend) handleActivityConfigUpdate(ctx context.Context, req *logical.Request, d *framework.FieldData) (*logical.Response, error) {
	a := b.Core.activityLog
	if a == nil {
		return logical.ErrorResponse("no activity log present"), nil
	}

	warnings := make([]string, 0)

	config, err := a.loadConfigOrDefault(ctx)
	if err != nil {
		return nil, err
	}

	{
		// Parse the default report months
		if defaultReportMonthsRaw, ok := d.GetOk("default_report_months"); ok {
			config.DefaultReportMonths = defaultReportMonthsRaw.(int)
		}

		if config.DefaultReportMonths <= 0 {
			return logical.ErrorResponse("default_report_months must be greater than 0"), logical.ErrInvalidRequest
		}
	}

	{
		// Parse the retention months
		if retentionMonthsRaw, ok := d.GetOk("retention_months"); ok {
			config.RetentionMonths = retentionMonthsRaw.(int)
		}

		if config.RetentionMonths < 0 {
			return logical.ErrorResponse("retention_months must be greater than or equal to 0"), logical.ErrInvalidRequest
		}

		if config.RetentionMonths > 36 {
			config.RetentionMonths = 36
			warnings = append(warnings, "retention_months cannot be greater than 36; capped to 36.")
		}
	}

	{
		// Parse the enabled setting
		if enabledRaw, ok := d.GetOk("enabled"); ok {
			enabledStr := enabledRaw.(string)

			// If we switch from enabled to disabled, then we return a warning to the client.
			// We have to keep the default state of activity log enabled in mind
			if config.Enabled == "enable" && enabledStr == "disable" ||
				!activityLogEnabledDefault && config.Enabled == "enable" && enabledStr == "default" ||
				activityLogEnabledDefault && config.Enabled == "default" && enabledStr == "disable" {
				warnings = append(warnings, "the current monthly segment will be deleted because the activity log was disabled")
			}

			switch enabledStr {
			case "default", "enable", "disable":
				config.Enabled = enabledStr
			default:
				return logical.ErrorResponse("enabled must be one of \"default\", \"enable\", \"disable\""), logical.ErrInvalidRequest
			}
		}
	}

	enabled := config.Enabled == "enable"
	if !enabled && config.Enabled == "default" {
		enabled = activityLogEnabledDefault
	}

	if enabled && config.RetentionMonths == 0 {
		return logical.ErrorResponse("retention_months cannot be 0 while enabled"), logical.ErrInvalidRequest
	}

	// Store the config
	entry, err := logical.StorageEntryJSON(path.Join(activitySubPath, activityConfigKey), config)
	if err != nil {
		return nil, err
	}
	if err := req.Storage.Put(ctx, entry); err != nil {
		return nil, err
	}

	// Set the new config on the activity log
	a.SetConfig(ctx, config)

	if len(warnings) > 0 {
		return &logical.Response{
			Warnings: warnings,
		}, nil
	}

	return nil, nil
}
