import { useNavigate } from "react-router";
import { User } from "lucide-react";

interface HeaderProps {
  userName?: string;
  userRole?: "admin" | "user";
}

export function Header({ userName, userRole }: HeaderProps) {
  const navigate = useNavigate();

  const handleProfileClick = () => {
    if (userRole === "admin") {
      navigate("/admin/profile");
    } else if (userRole === "user") {
      navigate("/user/profile");
    }
  };

  const handleTitleClick = () => {
    if (userRole === "admin") {
      navigate("/admin/dashboard");
    } else if (userRole === "user") {
      navigate("/user/dashboard");
    }
  };

  return (
    <header className="bg-[#1F3A5F] text-white px-10 py-6 flex justify-between items-center shadow-xl relative z-50">
      <h1 
        className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
        style={{ fontFamily: 'var(--font-head)', color: 'white' }}
        onClick={handleTitleClick}
      >
        STATXTRACT
      </h1>
      <div className="flex items-center gap-8">
        {userName && (
          <button
            onClick={handleProfileClick}
            className="lift-on-hover flex items-center gap-4 bg-[#2E7D32] border-2 border-[#2E7D32] text-white cursor-pointer px-6 py-3 rounded-xl transition-all duration-200 hover:border-[#F4A300] shadow-lg"
            style={{
              backgroundColor: userRole === 'admin' ? '#2E5BBA' : '#2E7D32',
              borderColor: userRole === 'admin' ? '#2E5BBA' : '#2E7D32',
              boxShadow: '0 4px 15px rgba(0,0,0,0.2)'
            }}
          >
            <div className="bg-white/20 p-2 rounded-full shadow-inner">
              <User className="w-6 h-6 text-white" />
            </div>
            <span className="font-bold text-xl">{userName}</span>
          </button>
        )}
      </div>
    </header>
  );
}