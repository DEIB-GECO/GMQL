import glob
import argparse
import xml.etree.ElementTree as ET

ET.register_namespace('', "http://maven.apache.org/POM/4.0.0")

ns = "{http://maven.apache.org/POM/4.0.0}"

ROOT_POM = "gmql"


def modifyChildrenPom(pom_xml, new_version, groupId):
	# modify the parent section
	pom_xml.find("./{}parent/{}version".format(ns, ns)).text = new_version

	# modify the dependencies
	for depenendencyNode in pom_xml.findall("./{}dependencies/".format(ns)):
		d_group_id = depenendencyNode.find("./{}groupId".format(ns)).text
		if groupId == d_group_id:
			depenendencyNode.find("./{}version".format(ns)).text = new_version


def modifyRootPom(root_pom_xml, new_version):
	versionNode = getVersionNode(root_pom_xml)
	versionNode.text = new_version


def getNewVersion(old_version, branch_name):
	if (old_version.endswith("SNAPSHOT")) & (branch_name != 'master'):
		parts = old_version.split("-")
		new_version = parts[0] + "-" + branch_name + "-" + parts[1]
	else:
		new_version = old_version
	return new_version


def getArtifactId(pom_f_xml):
	return pom_f_xml.find("./{}artifactId".format(ns)).text


def getGroupId(pom_f_xml):
	return pom_f_xml.find("./{}groupId".format(ns)).text


def getVersionNode(pom_f_xml):
	return pom_f_xml.find("./{}version".format(ns))


def getVersion(pom_f_xml):
	return getVersionNode(pom_f_xml).text


def isRootPom(pom_f_xml):
	return getArtifactId(pom_f_xml) == ROOT_POM


def main():
	parser = argparse.ArgumentParser(description='Modify the versions in the pom files')
	parser.add_argument('branch_name', help="Name of the branch of the current build")

	branch_name = parser.parse_args().branch_name
	if (branch_name.startswith("\"") & branch_name.endswith("\"")) | \
		(branch_name.startswith("'") & branch_name.endswith("'")):
		branch_name = branch_name[1:-1]


	print("The current branch is {}".format(branch_name))

	# root pom
	root_pom_path = "./pom.xml"
	root_pom_xml = ET.parse(root_pom_path)
	root_version = getVersion(root_pom_xml)
	root_group_id = getGroupId(root_pom_xml)
	root_artifact_id = getArtifactId(root_pom_xml)

	print("Root POM:\n\
		\tGroupId: {}\n\
		\tVersion: {}\n\
		\tArtifactId: {}".format(root_group_id, root_version, root_artifact_id))

	new_version = getNewVersion(root_version, branch_name)
	print("New Version: {}".format(new_version))

	modifyRootPom(root_pom_xml, new_version)
	root_pom_xml.write(root_pom_path)
	print("{:<30}{} --> {}".format(root_pom_path, root_version, new_version))
	# all the others
	other_pom_paths = glob.glob("./*/pom.xml")
	print("Changing POM files")
	for pom_path in other_pom_paths:
		pom_xml = ET.parse(pom_path)
		modifyChildrenPom(pom_xml, new_version, root_group_id)
		pom_xml.write(pom_path)
		print("{:<30}{} --> {}".format(pom_path, root_version, new_version))


if __name__ == '__main__':
	main()