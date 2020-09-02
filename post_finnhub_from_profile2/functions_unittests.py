import unittest
import io
import pandas as pd
from functions import *


class CheckStatusNewNameTest(unittest.TestCase):
    """Test case use to test function check_status_new_name"""

    def setUp(self):
        """ Setup the data from the database """
        
        lst_dict_data_base = \
        [{"name": "COMP A", 
        "logo": "https://static.finnhub.io/logo/comp_a", 
        "weburl": "https://comp_a.com/", 
        "phone": "123456789", 
        "date_ipo": "2018-05-03",
        "id_company":1},
         
        {"name": "COMP B", 
        "logo": "https://static.finnhub.io/logo/comp_b", 
        "weburl": "https://comp_b.com/", 
        "phone": "234567891", 
        "date_ipo": "2007-10-26",
        "id_company":2},

        {"name": "COMP C", 
        "logo": "https://static.finnhub.io/logo/comp_c",  
        "weburl": "https://comp_c.com/",  
        "phone": "3456789012", 
        "date_ipo": "1998-12-08",
        "id_company":3},

        {"name": "Comp D", 
        "logo": None, 
        "weburl": None, 
        "phone": None, 
        "date_ipo": "1995-05-15",
        "id_company":4},

        {"name": "Comp E", 
        "logo": "https://static.finnhub.io/logo/comp_e",  
        "weburl": "https://comp_e.com/",  
        "phone": "4567890123", 
        "date_ipo": "2012-04-27",
        "id_company":5}]  

        self.df_data_base = pd.DataFrame(lst_dict_data_base)
         
    def test_no_company_in_finnhub(self):
        """Test case where empty finnhub DataFrame"""
        columns=["name", "logo", "weburl", "phone", "ipo"]
        df_company_new_name = pd.DataFrame(columns=columns)
    
        df_comp_to_add, df_comp_names_changed = check_status_new_name(df_company_new_name, self.df_data_base, verbose=False)
        self.assertTrue((len(df_comp_to_add) == 0) and (len(df_comp_names_changed) == 0))
        

    def test_company_exists_name_and_phone_changed(self):      
        """Test case where the company exists, name changed and phone changed"""  
        dict_profile2 = \
        {"name": "COMP B NEW", 
        "logo": "https://static.finnhub.io/logo/comp_b", 
        "weburl": "https://comp_b.com/", 
        "phone": "234567891_new", 
        "ipo": "2007-10-26"}
        df_company_new_name = pd.DataFrame([dict_profile2])
        
        df_comp_to_add, df_comp_names_changed = check_status_new_name(df_company_new_name, self.df_data_base, verbose=False)
        self.assertTrue((len(df_comp_to_add) == 0) and (len(df_comp_names_changed) == 1))

    def test_company_exists_name_and_url_changed(self):      
        """Test case where the company exists, name changed and url changed """     
        dict_profile2 = \
        {"name": "COMP C NEW", 
        "logo": "https://static.finnhub.io/logo/comp_c",  
        "weburl": "https://comp_c_new.com/",  
        "phone": "3456789012", 
        "ipo": "1998-12-08"}
        df_company_new_name = pd.DataFrame([dict_profile2])

        df_comp_to_add, df_comp_names_changed = check_status_new_name(df_company_new_name, self.df_data_base, verbose=False)
        self.assertTrue((len(df_comp_to_add) == 0) and (len(df_comp_names_changed) == 1))


    def test_company_exists_name_and_url_none(self):      
        """Test case where the company exists, name changed and url/phone fields still None """     
        dict_profile2 = \
        {"name": "COMP D NEW", 
        "logo": None, 
        "weburl": None, 
        "phone": None, 
        "ipo": "1995-05-15"}
        df_company_new_name = pd.DataFrame([dict_profile2])

        df_comp_to_add, df_comp_names_changed = check_status_new_name(df_company_new_name, self.df_data_base, verbose=False)
        self.assertTrue((len(df_comp_to_add) == 1) and (len(df_comp_names_changed) == 0))
        
      
    def test_company_exists_only_name_has_changed(self):      
        """Test case where the company exists, only the name has changed"""     
        dict_profile2 = \
        {"name": "COMP E NEW", 
        "logo": "https://static.finnhub.io/logo/comp_e",  
        "weburl": "https://comp_e.com/",  
        "phone": "4567890123", 
        "ipo": "2012-04-27"}
        df_company_new_name = pd.DataFrame([dict_profile2])

        df_comp_to_add, df_comp_names_changed = check_status_new_name(df_company_new_name, self.df_data_base, verbose=False)
        self.assertTrue((len(df_comp_to_add) == 0) and (len(df_comp_names_changed) == 1))  
    

    def test_totally_new_company(self):      
        """Test case where there is a totally new company"""     
        dict_profile2 = \
        {"name": "COMP F", 
        "logo": "https://static.finnhub.io/logo/comp_f",  
        "weburl": "https://comp_f.com/",  
        "phone": "5678901234", 
        "ipo": "2020-08-27"}
        df_company_new_name = pd.DataFrame([dict_profile2])

        df_comp_to_add, df_comp_names_changed = check_status_new_name(df_company_new_name, self.df_data_base, verbose=False)
        self.assertTrue((len(df_comp_to_add) == 1) and (len(df_comp_names_changed) == 0))
        

    def test_empty_database(self):      
        """Test case where the database is empty"""     
        dict_profile2 = \
        {"name": "COMP F", 
        "logo": "https://static.finnhub.io/logo/comp_f",  
        "weburl": "https://comp_f.com/",  
        "phone": "5678901234", 
        "ipo": "2020-08-27"}
        df_company_new_name = pd.DataFrame([dict_profile2])
        df_data_base = pd.DataFrame(columns=["name", "logo", "weburl", "phone", "ipo"])
        
        df_comp_to_add, df_comp_names_changed = check_status_new_name(df_company_new_name, self.df_data_base, verbose=False)
        self.assertTrue((len(df_comp_to_add) == 1) and (len(df_comp_names_changed) == 0))

        
class KeepNewNamesTest(unittest.TestCase):
    """Test case use to test function keep_new_names"""
    
    def setUp(self):
        """ Setup the data from the database """
        
        lst_dict_data_base = \
        [{"name": "COMP A", 
        "logo": "https://static.finnhub.io/logo/comp_a", 
        "weburl": "https://comp_a.com/", 
        "phone": "123456789", 
        "date_ipo": "2018-05-03",
        "id_company":1},
         
        {"name": "COMP B", 
        "logo": "https://static.finnhub.io/logo/comp_b", 
        "weburl": "https://comp_b.com/", 
        "phone": "234567891", 
        "date_ipo": "2007-10-26",
        "id_company":2},

        {"name": "COMP C", 
        "logo": "https://static.finnhub.io/logo/comp_c",  
        "weburl": "https://comp_c.com/",  
        "phone": "3456789012", 
        "date_ipo": "1998-12-08",
        "id_company":3},

        {"name": "Comp D", 
        "logo": None, 
        "weburl": None, 
        "phone": None, 
        "date_ipo": "1995-05-15",
        "id_company":4},

        {"name": "Comp E", 
        "logo": "https://static.finnhub.io/logo/comp_e",  
        "weburl": "https://comp_e.com/",  
        "phone": "4567890123", 
        "date_ipo": "2012-04-27",
        "id_company":5}]  

        self.df_data_base = pd.DataFrame(lst_dict_data_base)
        
    
    def test_no_company_in_finnhub(self):
        """Test case where empty finnhub DataFrame"""
        columns=["name", "logo", "weburl", "phone", "ipo"]
        df_company_profile2 = pd.DataFrame(columns=columns)
    
        df_company_new_name = keep_new_names(df_company_profile2, self.df_data_base, verbose=False)
        self.assertEqual(len(df_company_new_name), 0)
        
        
    def test_no_new_company_in_finnhub(self):
        """Test case where the company exists""" 
        dict_profile2 = \
        {"name": "COMP A", 
        "logo": "https://static.finnhub.io/logo/comp_a", 
        "weburl": "https://comp_a.com/", 
        "phone": "123456789", 
        "ipo": "2018-05-03"}
        df_company_profile2 = pd.DataFrame([dict_profile2])
        
        df_company_new_name = keep_new_names(df_company_profile2, self.df_data_base, verbose=False)
        self.assertEqual(len(df_company_new_name), 0)
        
        
    def test_company_exists_name_and_phone_changed(self):      
        """Test case where the company exists, name changed and phone changed"""  
        dict_profile2 = \
        {"name": "COMP B NEW", 
        "logo": "https://static.finnhub.io/logo/comp_b", 
        "weburl": "https://comp_b.com/", 
        "phone": "234567891_new", 
        "ipo": "2007-10-26"}
        df_company_profile2 = pd.DataFrame([dict_profile2])
        
        df_company_new_name = keep_new_names(df_company_profile2, self.df_data_base, verbose=False)
        self.assertEqual(df_company_new_name.loc[0, "name"], "COMP B NEW")

    def test_company_exists_name_and_url_changed(self):      
        """Test case where the company exists, name changed and url changed """     
        dict_profile2 = \
        {"name": "COMP C NEW", 
        "logo": "https://static.finnhub.io/logo/comp_c",  
        "weburl": "https://comp_c_new.com/",  
        "phone": "3456789012", 
        "ipo": "1998-12-08"}
        df_company_profile2 = pd.DataFrame([dict_profile2])

        df_company_new_name = keep_new_names(df_company_profile2, self.df_data_base, verbose=False)
        self.assertEqual(df_company_new_name.loc[0, "name"], "COMP C NEW")


    def test_company_exists_name_and_url_none(self):      
        """Test case where the company exists, name changed and url/phone fields still None """     
        dict_profile2 = \
        {"name": "COMP D NEW", 
        "logo": None, 
        "weburl": None, 
        "phone": None, 
        "ipo": "1995-05-15"}
        df_company_profile2 = pd.DataFrame([dict_profile2])

        df_company_new_name = keep_new_names(df_company_profile2, self.df_data_base, verbose=False)
        self.assertEqual(df_company_new_name.loc[0, "name"], "COMP D NEW")
        
      
    def test_company_exists_only_name_has_changed(self):      
        """Test case where the company exists, only the name has changed"""     
        dict_profile2 = \
        {"name": "COMP E NEW", 
        "logo": "https://static.finnhub.io/logo/comp_e",  
        "weburl": "https://comp_e.com/",  
        "phone": "4567890123", 
        "ipo": "2012-04-27"}
        df_company_profile2 = pd.DataFrame([dict_profile2])

        df_company_new_name = keep_new_names(df_company_profile2, self.df_data_base, verbose=False)
        self.assertEqual(df_company_new_name.loc[0, "name"], "COMP E NEW")  
    

    def test_totally_new_company(self):      
        """Test case where there is a totally new company"""     
        dict_profile2 = \
        {"name": "COMP F", 
        "logo": "https://static.finnhub.io/logo/comp_f",  
        "weburl": "https://comp_f.com/",  
        "phone": "5678901234", 
        "ipo": "2020-08-27"}
        df_company_profile2 = pd.DataFrame([dict_profile2])

        df_company_new_name = keep_new_names(df_company_profile2, self.df_data_base, verbose=False)
        self.assertEqual(df_company_new_name.loc[0, "name"], "COMP F")
        

    def test_empty_database(self):      
        """Test case where the database is empty"""     
        dict_profile2 = \
        {"name": "COMP F", 
        "logo": "https://static.finnhub.io/logo/comp_f",  
        "weburl": "https://comp_f.com/",  
        "phone": "5678901234", 
        "ipo": "2020-08-27"}
        df_company_profile2 = pd.DataFrame([dict_profile2])
        df_data_base = pd.DataFrame(columns=["name", "logo", "weburl", "phone", "ipo"])
        
        df_company_new_name = keep_new_names(df_company_profile2, df_data_base, verbose=False)
        self.assertEqual(df_company_new_name.loc[0, "name"], "COMP F")

        
def get_unittest_dataframe():
    # Redirect stdout
    old_stdout = sys.stdout
    new_stdout = io.StringIO()
    sys.stdout = new_stdout

    # Run only the tests in the specified classe
    test_classes_to_run = [CheckStatusNewNameTest, KeepNewNamesTest]

    loader = unittest.TestLoader()
    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    # launch the tests
    big_suite = unittest.TestSuite(suites_list)
    runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=2)
    results = runner.run(big_suite)

    # Get value from stdout then redirect it 
    output = new_stdout.getvalue()
    sys.stdout = old_stdout

    # Separate the final summary from the last error message or check list
    final = output.split('\n\n----------------------------------------------------------------------\nRan')[-1]
    rest = "".join(output.split('\n\n----------------------------------------------------------------------\nRan')[:-1])
    final = 'Ran' + final

    # Separate the check list from the error message panel
    main_parts = rest.split('\n\n======================================================================\n')
    main_parts.append(final)

    # Deal with the checklist
    lines = main_parts[0].split('\n')
    df = pd.DataFrame(lines, columns=["line"])

    for ind in df.index:
        if int(ind / 2) * 2 == ind:
            line_function = df.loc[ind, "line"]

            function = line_function.split("functions_unittests.")[1][:-1]
            df.loc[ind, "function"] = function
            df.loc[ind + 1, "function"] = function

            test_name = line_function.split(" (functions_unittests.")[0]
            df.loc[ind, "test_name"] = test_name
            df.loc[ind + 1, "test_name"] = test_name

            line_test_result = df.loc[ind + 1, "line"]
            test_caption = line_test_result.split(" ... ")[0]
            df.loc[ind, "test_caption"] = test_caption
            df.loc[ind + 1, "test_caption"] = test_caption

            test_result = line_test_result.split(" ... ")[1]
            df.loc[ind, "test_result"] = test_result
            df.loc[ind + 1, "test_result"] = "DROP_LINE"
    df = df[df["test_result"] != "DROP_LINE"]

    # Deals with the error message
    for i in range(len(main_parts) - 1):
        if i != 0:
            main_part = main_parts[i]
            message = main_part.split("\n----------------------------------------------------------------------\n")[1]
            fail_id = main_part.split("\n----------------------------------------------------------------------\n")[0]
            test_caption = fail_id.split("\n")[1] 
            line1 = fail_id.split("\n")[0]
            function = line1.split("(functions_unittests.")[1][:-1]
            test_name = line1.split(" (functions_unittests.")[0][6:]
            test_name = test_name.strip()

            condition = (df["function"] == function) & \
                        (df["test_caption"] == test_caption) & \
                        (df["test_name"] == test_name)
            df.loc[condition, ["error_message"]] = message

    # Deals with the summary
    df["summary"] = main_parts[-1]
    df = df.drop(columns=["line"])
    
    return df


def display_unittest(df):
    # Get list of functions tested
    for funct in df["function"].unique():
        print("\033[1m{}\033[0m".format(funct))
        sub_df_by_funct = df.loc[df["function"] == funct].copy()
        for ind in sub_df_by_funct.index:
            result = sub_df_by_funct.loc[ind, "test_result"]
            if result == "ok":
                print('\t\033[1m\033[92m[OK]\x1b[0m\033[0m {}'.format(sub_df_by_funct.loc[ind, "test_caption"]))
            elif result == "ERROR":
                print('\t\033[1m\033[91m[ERROR]\x1b[0m\033[0m {}'.format(sub_df_by_funct.loc[ind, "test_caption"]))
                error_message = sub_df_by_funct.loc[ind, "error_message"]
                lines = error_message.split('\n')
                for line in lines[:-1]:
                    print('\t{}'.format(line))
            else:
                print('\t\033[93m\033[91m[FAIL]\x1b[0m\033[0m {}'.format(sub_df_by_funct.loc[ind, "test_caption"]))
                error_message = sub_df_by_funct.loc[ind, "error_message"]
                lines = error_message.split('\n')
                for line in lines[:-1]:
                    print('\t{}'.format(line))   
    print("------------------------")
    summary = df.loc[0, "summary"]
    summary = summary.replace("\n\n", "\n")
    print("\033[1m{}\033[0m".format(summary))