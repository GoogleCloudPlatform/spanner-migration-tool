package assessment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	sitter "github.com/smacker/go-tree-sitter"
	"github.com/smacker/go-tree-sitter/java"
	"go.uber.org/zap"
)

// JavaDependencyAnalyzer implements DependencyAnalyzer for Go projects
type JavaDependencyAnalyzer struct {
	BaseAnalyzer
	ctx context.Context
}

type JavaFileParsedInfo struct {
	FileName        string
	FilePath        string
	Package         string
	DeclaredClasses []string
	FileContent     []byte
}

func (g *JavaDependencyAnalyzer) IsDAO(filePath string, fileContent string) bool {
	filePath = strings.ToLower(filePath)
	if strings.Contains(filePath, "dao") {
		return true
	}

	fileContentLowerCase := strings.ToLower(fileContent)

	if strings.Contains(fileContentLowerCase, "jdbc") || strings.Contains(fileContentLowerCase, "mysql") {
		return true
	}

	return false
}

func (g *JavaDependencyAnalyzer) GetFrameworkFromFileContent(fileContent string) string {
	if strings.Contains(fileContent, "org.hibernate") {
		return "Hibernate"
	}
	if strings.Contains(fileContent, "org.apache.ibatis") {
		return "MyBatis"
	}
	if strings.Contains(fileContent, "java.sql.DriverManager") || strings.Contains(fileContent, "javax.sql.DataSource") {
		return "JDBC"
	}
	if strings.Contains(fileContent, "org.springframework.data.jpa") {
		return "Spring Data JPA"
	}
	return ""
}

func (j *JavaDependencyAnalyzer) GetExecutionOrder(projectDir string) (map[string]map[string]struct{}, [][]string) {
	G := j.getDependencyGraph(projectDir)

	sortedTasks, err := j.TopologicalSort(G)
	if err != nil {
		logger.Log.Debug("Graph still has cycles after relaxation. Sorting not possible: ", zap.Error(err))
		return nil, nil
	}

	logger.Log.Debug("Execution order determined successfully.")
	return G, sortedTasks
}

// getDependencyGraph: get dependency graph for java files. There will be not cycle in the graph
func (j *JavaDependencyAnalyzer) getDependencyGraph(directory string) map[string]map[string]struct{} {

	parser := sitter.NewParser()
	defer parser.Close()

	parser.SetLanguage(java.GetLanguage())

	fileDependenciesMapWithCycles := make(map[string]map[string]struct{})
	classToFileInfosMap, fileInfoPathMap, err := fetchedFileClassPackageMap(j.ctx, parser, directory)
	if err != nil {
		logger.Log.Error("Error walking the directory while parsing java file for declared classes:", zap.Error(err))
		return fileDependenciesMapWithCycles
	}

	err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".java") {
			return nil
		}

		fileInfo, ok := fileInfoPathMap[path]
		if !ok {
			logger.Log.Error("Error fetching file parsed info:", zap.String("path", path))
		}
		fileDependenciesMapWithCycles[path] = make(map[string]struct{})

		referencedClassAndPackages, err := fetchClassReferences(j.ctx, parser, fileInfo.FileContent)
		if err != nil {
			logger.Log.Error("Error fetching class references:", zap.String("path", path), zap.Error(err))
			return nil
		}
		for referencedClassIndex := range referencedClassAndPackages {
			referencedClass := resolveClassDependencies(classToFileInfosMap, fileInfo.Package, referencedClassAndPackages, referencedClassIndex)
			if referencedClass != "" {
				fileDependenciesMapWithCycles[path][referencedClass] = struct{}{}
			}
		}
		return nil
	})

	if err != nil {
		logger.Log.Error("Error walking the directory while parsing java file for declared classes:", zap.Error(err))
		return fileDependenciesMapWithCycles
	}

	return j.RemoveCycle(fileDependenciesMapWithCycles)
}

// resolveClassDependencies: within referencedClasses, tries to map the class at referencedClassIndex to the file path.
// If the class name is not fully qualified, then the function tries to map the class name to the file path.
func resolveClassDependencies(classFileInfoMap map[string][]*JavaFileParsedInfo, sourcePackage string, referencedClasses []string, referencedClassIndex int) string {

	referencedClass := referencedClasses[referencedClassIndex]
	parsedFileInfos, ok := classFileInfoMap[referencedClass]
	if !ok {
		return ""
	}

	isReferencedClassPackage := isPackage(&referencedClass)
	for _, parsedFileInfo := range parsedFileInfos {
		if isReferencedClassPackage && strings.HasPrefix(referencedClass, parsedFileInfo.Package) {
			return parsedFileInfo.FilePath
		}
		if isResolvedClassNameEqual(referencedClasses, referencedClassIndex, parsedFileInfo.Package, isReferencedClassPackage) {
			return parsedFileInfo.FilePath
		}
		if parsedFileInfo.Package == sourcePackage {
			return parsedFileInfo.FilePath
		}
	}

	return ""
}

func isResolvedClassNameEqual(referencedClasses []string, referencedClassIndex int, targetPackage string, isPackage bool) bool {
	if isPackage {
		return false
	}
	targetPackageLength := strings.Count(targetPackage, ".") + 1

	packageStartIndex := referencedClassIndex - targetPackageLength
	if packageStartIndex < 0 {
		return false
	}

	targetPackageParts := strings.Split(targetPackage, ".")
	for i := packageStartIndex; i < referencedClassIndex; i++ {
		if targetPackageParts[i-packageStartIndex] != referencedClasses[i] {
			return false
		}
	}
	return true
}

func isPackage(reference *string) bool {
	return strings.Contains(*reference, ".")
}

// fetchedFileClassPackageMap: parses java files within projectDir to fetch declared classes and package name for each
// file. The output is structured in 2 format.
// map[string]*JavaFileParsedInfo: map of parsed info with File path as key.
// map[string][]*JavaFileParsedInfo: map of parsed info with class name as key. Value is a list of files that declared the class.
func fetchedFileClassPackageMap(ctx context.Context, parser *sitter.Parser, projectDir string) (map[string][]*JavaFileParsedInfo, map[string]*JavaFileParsedInfo, error) {

	fileParsedInfo := make([]*JavaFileParsedInfo, 0, 10)
	fileClassPackageMap := make(map[string][]*JavaFileParsedInfo)
	fileInfoPathMap := make(map[string]*JavaFileParsedInfo)

	err := filepath.Walk(projectDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".java") {
			parsedInfo, err := fetchFileParsedInfo(ctx, parser, path, projectDir)
			if err != nil {
				logger.Log.Error("Error fetching file parsed info:", zap.String("path", path), zap.Error(err))
			} else {
				fileParsedInfo = append(fileParsedInfo, parsedInfo)
				fileInfoPathMap[path] = parsedInfo
				for _, className := range parsedInfo.DeclaredClasses {
					if fileInfos, ok := fileClassPackageMap[className]; ok {
						fileClassPackageMap[className] = append(fileInfos, parsedInfo)
					} else {
						fileClassPackageMap[className] = []*JavaFileParsedInfo{parsedInfo}
					}
					classPath := fmt.Sprintf("%s.%s", parsedInfo.Package, className)
					if fileInfos, ok := fileClassPackageMap[classPath]; ok {
						fileClassPackageMap[classPath] = append(fileInfos, parsedInfo)
					} else {
						fileClassPackageMap[classPath] = []*JavaFileParsedInfo{parsedInfo}
					}
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return fileClassPackageMap, fileInfoPathMap, nil
}

// fetchFileParsedInfo: parses java file and returns parsed info containing file name, file path, package name, declared classes, and file content.
func fetchFileParsedInfo(ctx context.Context, parser *sitter.Parser, filePath string, projectDir string) (*JavaFileParsedInfo, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	rootNode := tree.RootNode()

	packageName, err := fetchPackageName(rootNode, content)
	if err != nil {
		return nil, err
	}

	declaredClasses, err := fetchClassDeclaration(rootNode, content)
	if err != nil {
		return nil, err
	}

	return &JavaFileParsedInfo{
		FileName:        filepath.Base(filePath),
		FilePath:        filePath,
		Package:         packageName,
		DeclaredClasses: declaredClasses,
		FileContent:     content,
	}, nil

}

// fetchClassReferences: parses java file and returns a list of class references. This excludes the classes which are
// referred using fully qualified package name.
func fetchClassReferences(ctx context.Context, parser *sitter.Parser, content []byte) ([]string, error) {
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	rootNode := tree.RootNode()

	query, err := sitter.NewQuery([]byte(`
                (
                        (type_identifier) @class_ref
                        (#not-match? @class_ref "^(void|int|double|float|boolean|char|byte|short|long)$")
                )
                (
                        (scoped_identifier) @class_ref
                )
        `), java.GetLanguage())

	if err != nil {
		return nil, err
	}
	defer query.Close()

	qc := sitter.NewQueryCursor()
	defer qc.Close()

	qc.Exec(query, rootNode)

	var references []string
	for {
		match, found := qc.NextMatch()
		if !found {
			break
		}
		for _, capture := range match.Captures {
			references = append(references, capture.Node.Content(content))
		}
	}
	return references, nil

}

// fetchPackageName: parses java file and returns the package name of the file.
func fetchPackageName(rootNode *sitter.Node, content []byte) (string, error) {
	packageQuery, err := sitter.NewQuery([]byte(`(package_declaration (scoped_identifier) @package)`), java.GetLanguage())

	if err != nil {
		return "", err
	}
	defer packageQuery.Close()

	packageCursor := sitter.NewQueryCursor()
	defer packageCursor.Close()

	packageCursor.Exec(packageQuery, rootNode)

	packageName := ""

	if match, found := packageCursor.NextMatch(); found {
		for _, capture := range match.Captures {
			return capture.Node.Content(content), nil
		}
	}
	return packageName, nil
}

// fetchClassDeclaration: parses java file and returns a list of declared classes and interface in the file.
func fetchClassDeclaration(rootNode *sitter.Node, content []byte) ([]string, error) {
	classQuery, err := sitter.NewQuery([]byte(`
		(class_declaration (identifier) @class)
		(interface_declaration (identifier) @interface_name)
	`), java.GetLanguage())

	if err != nil {
		return nil, err
	}

	defer classQuery.Close()

	classCursor := sitter.NewQueryCursor()
	defer classCursor.Close()

	classCursor.Exec(classQuery, rootNode)

	var classNames []string

	for {
		match, found := classCursor.NextMatch()
		if !found {
			break
		}
		for _, capture := range match.Captures {
			classNames = append(classNames, capture.Node.Content(content))
		}
	}
	return classNames, nil
}
