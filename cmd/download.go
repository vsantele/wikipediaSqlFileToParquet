package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vsantele/wikipediaSqlFileToParquet/download"
	"github.com/vsantele/wikipediaSqlFileToParquet/validate"
)

// downloadCmd represents the download command
var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download the sql.gz files from the Wikimedia dumps",
	Long: `Download the sql.gz files from the Wikimedia dumps

	Example: ./wikipediaSqlFileToParquet download --root /tmp --language en --date 20210801 page pagelinks redirect`,
	Args: func(cmd *cobra.Command, args []string) error {
		// Optionally run one of the validators provided by cobra
		if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
			return err
		}

		for _, arg := range args {
			if !validate.IsTableNameValid(arg) {
				return fmt.Errorf("invalid table specified: %s", arg)
			}
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		language, _ := cmd.Flags().GetString("language")
		root, _ := cmd.Flags().GetString("root")
		date, _ := cmd.Flags().GetString("date")
		download.Download(root, language, date, args)
	},
}

func init() {
	rootCmd.AddCommand(downloadCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// downloadCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// downloadCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
