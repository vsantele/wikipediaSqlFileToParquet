package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/vsantele/wikipediaSqlFileToParquet/process"
	"github.com/vsantele/wikipediaSqlFileToParquet/validate"
)

// convertCmd represents the convert command
var convertCmd = &cobra.Command{
	Use:   "convert",
	Short: "Convert SQL file to parquet file",
	Long:  `Convert table from args from SQL to parquet`,
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
		process.Process(root, language, date, args)

		fmt.Println("convert called")

	},
}

func init() {
	rootCmd.AddCommand(convertCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// convertCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// convertCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
